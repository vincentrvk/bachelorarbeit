/*****************************************************************************
 *  Groovy-Skript:  Replikation von B2C-Contact-Updates in die SAP CDC
 *  Autor:          AI – Senior-Entwickler Integrations (SAP CPI)
 *  Beschreibung:   Siehe detaillierte Aufgabenstellung
 *****************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.net.HttpURLConnection
import java.net.URLEncoder

/*****************************************************************************
 *  Haupteinstieg
 *****************************************************************************/
Message processData(Message message) {

    def messageLog = messageLogFactory?.getMessageLog(message)

    try {

        /* 1. Konfigurationswerte aus Header/Property lesen  */
        def cfg = setConfigValues(message, messageLog)

        /* 2. Eingangs-XML parsen                                        */
        String xmlBody = message.getBody(String)
        def xml      = new XmlSlurper().parseText(xmlBody)

        /* 3. Je BusinessPartner Element verarbeiten (Splitter-Logik)    */
        xml.'**'.findAll { it.name() == 'BusinessPartner' }.each { bp ->
            processBusinessPartnerElement(bp, cfg, message, messageLog)
        }

        /* 4. Rückgabe der (unveränderten) Nachricht                     */
        return message

    } catch (Exception ex) {
        /* 5. Zentrales Fehler-Handling                                   */
        handleError(message.getBody(String) as String, ex, messageLog)
        return message          // wird durch Exception nie erreicht
    }
}

/*****************************************************************************
 *  Funktion: Konfiguration aus Headern & Properties holen
 *****************************************************************************/
Map setConfigValues(Message message, def log) {

    /* Helper zum komfortablen Auslesen                                */
    def read = { source, key -> source?.containsKey(key) ? source[key] : 'placeholder' }

    Map props = message.getProperties()
    Map hdrs  = message.getHeaders()

    Map cfg = [
            requestUser            : read(props, 'requestUser'),
            requestPassword        : read(props, 'requestPassword'),
            requestURL             : read(props, 'requestURL'),
            plainHMACKeyForSignature: read(props, 'plainHMACKeyForSignature'),
            apiKey                 : read(props, 'apiKey')
    ]

    /* Falls Header gepflegt, Placeholder überschreiben                */
    cfg.keySet().each { k ->
        if (cfg[k] == 'placeholder') {
            cfg[k] = read(hdrs, k)
        }
    }

    log?.addAttachmentAsString('ConfigValues', cfg.toString(), 'text/plain')
    return cfg
}

/*****************************************************************************
 *  Funktion: BusinessPartner-Element verarbeiten
 *****************************************************************************/
void processBusinessPartnerElement(def bp,
                                   Map cfg,
                                   Message message,
                                   def log) {

    /* 1. Stammdaten aus dem XML ziehen                                */
    String profileID = bp.Common.IAMID.text()
    String role      = bp.Role.RoleCode.text()

    message.setProperty('profileID', profileID)
    message.setProperty('role',      role)

    /* 2. JWT-Token & HMAC-Signatur erstellen                           */
    String jwt       = createJwtToken(cfg.apiKey)
    String signature = signToken(jwt, cfg.plainHMACKeyForSignature)

    /* 3. Profil aus BusinessPartner-XML in Map überführen              */
    Map newProfile   = mapBusinessPartnerToProfile(bp)

    /* 4. GET Account in CDC                                            */
    Map existingProfile = getAccount(cfg, role, profileID, jwt, signature, log)

    /* 5. Vergleich alt/neu & Ermittlung zu aktualisierender Felder     */
    Map updatedProfile = compareProfiles(newProfile, existingProfile)

    /* 6. Body für POST Update zusammenbauen                            */
    String requestBody = buildUpdatePayload(cfg.apiKey, profileID, updatedProfile)

    /* 7. POST Update Profile                                           */
    updateProfile(cfg, profileID, jwt, signature, requestBody, log)
}

/*****************************************************************************
 *  Funktion: JWT-Token (Header.Claim) bauen
 *****************************************************************************/
String createJwtToken(String apiKey) {

    def header = [alg: 'RS256', typ: 'JWT', kid: apiKey]
    def claim  = [iat: (System.currentTimeMillis() / 1000L) as long]

    def b64    = { String s -> s.bytes.encodeBase64().toString() }

    return "${b64(JsonOutput.toJson(header))}.${b64(JsonOutput.toJson(claim))}"
}

/*****************************************************************************
 *  Funktion: JWT-Token per HMAC-SHA256 signieren (CDCSignature)
 *****************************************************************************/
String signToken(String token, String plainKey) {

    Mac mac = Mac.getInstance('HmacSHA256')
    mac.init(new SecretKeySpec(plainKey.bytes, 'HmacSHA256'))
    byte[] rawHmac = mac.doFinal(token.bytes)

    return rawHmac.encodeBase64().toString()
}

/*****************************************************************************
 *  Funktion: BusinessPartner-XML → Profil-Map
 *****************************************************************************/
Map mapBusinessPartnerToProfile(def bp) {

    /* Aktuell werden nur Beispiel-Felder gemappt. Bei Bedarf erweitern */
    return [
            firstName: bp.Name.FirstName.text(),
            lastName : bp.Name.LastName.text(),
            city     : bp.Address.City.text()
    ]
}

/*****************************************************************************
 *  Funktion: GET Account aus CDC
 *****************************************************************************/
Map getAccount(Map cfg,
               String role,
               String profileID,
               String jwt,
               String signature,
               def log) {

    String urlStr = "${cfg.requestURL}/${role}/${profileID}"
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()

    String auth = "${cfg.requestUser}:${cfg.requestPassword}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${auth}")
    conn.setRequestProperty('JWT-Token',      jwt)
    conn.setRequestProperty('CDCSignature',   signature)
    conn.requestMethod = 'GET'

    int rc = conn.responseCode

    if (rc == 200) {
        String response = conn.inputStream.text
        log?.addAttachmentAsString("GetAccount_${profileID}", response, 'application/json')

        def json = new JsonSlurper().parseText(response)

        /* Prospect-Response hat results-Array, Customer nicht          */
        if (json?.profile) {
            return json.profile as Map
        } else if (json?.results) {
            def pr = json.results.find { it.profile }
            return pr ? pr.profile as Map : null
        }
        return null

    } else if (rc == 404) {
        /* Kein Profil vorhanden – das ist zulässig                     */
        return null
    } else {
        throw new RuntimeException("GET Account ${urlStr} schlug fehl (HTTP ${rc})")
    }
}

/*****************************************************************************
 *  Funktion: Profile vergleichen & Änderungen ermitteln
 *****************************************************************************/
Map compareProfiles(Map newProfile, Map existingProfile) {

    if (existingProfile == null) {
        /* Neuer Kontakt                                                */
        return newProfile
    }

    Map changed = [:]
    newProfile.each { k, v ->
        if (!existingProfile.containsKey(k) || existingProfile[k] != v) {
            changed[k] = v
        }
    }

    if (changed.isEmpty()) {
        throw new RuntimeException('Keine Änderungen am Profil festgestellt')
    }

    /* Vollständiges Profil (alt + Änderungen) zurückgeben             */
    Map updated = [:]
    updated.putAll(existingProfile)
    updated.putAll(changed)
    return updated
}

/*****************************************************************************
 *  Funktion: Update-Payload erzeugen
 *****************************************************************************/
String buildUpdatePayload(String apiKey, String profileID, Map profileMap) {

    String enc = { String v -> URLEncoder.encode(v, 'UTF-8') }

    return "apiKey=${enc(apiKey)}" +
           "&UID=${enc(profileID)}" +
           "&profile=${enc(JsonOutput.toJson(profileMap))}"
}

/*****************************************************************************
 *  Funktion: POST Update Profile an CDC
 *****************************************************************************/
void updateProfile(Map cfg,
                   String profileID,
                   String jwt,
                   String signature,
                   String body,
                   def log) {

    String urlStr = "${cfg.requestURL}/${profileID}"
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()

    /* Header setzen                                                  */
    conn.requestMethod = 'POST'
    conn.doOutput      = true

    String auth = "${cfg.requestUser}:${cfg.requestPassword}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization',  "Basic ${auth}")
    conn.setRequestProperty('JWT-Token',      jwt)
    conn.setRequestProperty('CDCSignature',   signature)
    conn.setRequestProperty('Content-Type',   'application/x-www-form-urlencoded')

    /* Body schreiben                                                 */
    conn.outputStream.withWriter('UTF-8') { it << body }

    int rc = conn.responseCode
    log?.addAttachmentAsString("UpdateProfileResponse_${profileID}",
                               "HTTP ${rc}", 'text/plain')

    if (!(rc in [200, 201])) {
        throw new RuntimeException("POST Update Profile schlug fehl (HTTP ${rc})")
    }
}

/*****************************************************************************
 *  Funktion: Zentrales Fehler-Handling
 *  Anhängen des fehlerhaften Payloads an die Nachricht
 *****************************************************************************/
void handleError(String body, Exception e, def messageLog) {

    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    String errMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}