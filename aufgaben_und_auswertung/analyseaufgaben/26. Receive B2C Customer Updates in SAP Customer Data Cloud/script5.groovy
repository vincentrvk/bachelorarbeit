/******************************************************************************************
 *  Groovy-Skript – SAP Cloud Integration
 *  Replikation von B2C-Contact-Updates aus SAP Master Data Integration
 *  in die SAP Customer Data Cloud (CDC)
 *
 *  Autor:   Senior Integration Developer
 *  Version: 1.0
 *
 *  ACHTUNG:
 *  • Das Skript wird pro BusinessPartner (Splitter!) ausgeführt.
 *  • Alle benötigten Properties / Header werden (sofern nicht vorhanden)
 *    mit „placeholder“ vorbelegt.
 ******************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.net.HttpURLConnection
import java.net.URL

/* ========================================================================================
 *  HAUPTEINSTIEG
 * ====================================================================================== */
Message processData(Message message) {

    /* Body (XML des einzelnen BusinessPartner) einlesen */
    String body       = message.getBody(String) ?: ''
    def   messageLog  = messageLogFactory.getMessageLog(message)

    try {
        /* 1) Properties / Header initialisieren */
        initContext(message)

        /* 2) Profil-Informationen aus XML extrahieren */
        def xml          = new XmlSlurper().parseText(body)
        String profileID = xml.BusinessPartner.Common.IAMID.text()
        String role      = xml.BusinessPartner.Role.RoleCode.text()

        if (!profileID) throw new RuntimeException('Fehlende IAMID im Payload.')
        if (!role)      throw new RuntimeException('Fehlender RoleCode im Payload.')

        message.setProperty('profileID', profileID)
        message.setProperty('role',      role)

        /* 3) JWT-Token & CDC-Signature erstellen */
        String apiKey    = message.getProperty('apiKey')
        String jwtToken  = generateJwtToken(apiKey)
        String signature = signToken(jwtToken,
                                     message.getProperty('plainHMACKeyForSignature'))

        /* 4) GET Account */
        Map existingProfile = callGetAccount(message, jwtToken, signature,
                                             profileID, role, messageLog)

        /* 5) Eingehendes Profil aus XML bilden */
        Map incomingProfile = buildIncomingProfile(xml)

        /* 6) Vergleich Profile */
        boolean updateNeeded = (existingProfile == null) ||
                               !profilesEqual(incomingProfile, existingProfile)
        if (!updateNeeded) {
            throw new RuntimeException("Keine Änderungen für Profil ${profileID} festgestellt.")
        }

        /* 7) Profil zusammenführen (neu ODER aktualisierte Felder) */
        Map mergedProfile = (existingProfile ?: [:]) + incomingProfile   // incoming > existing

        /* 8) POST Update Profile */
        callUpdateProfile(message, jwtToken, signature,
                          profileID, mergedProfile, messageLog)

    } catch (Exception e) {
        /* Zentrales Error-Handling */
        handleError(body, e, messageLog)
    }
    return message
}

/* ========================================================================================
 *  FUNKTIONEN
 * ====================================================================================== */

/* ----------------------------------------------------------------------------
 *  initContext – Properties & Header vorbelegen
 * ------------------------------------------------------------------------- */
void initContext(Message message) {
    /* Liste der erwarteten Properties / Header mit Default „placeholder“      */
    ['requestUser',
     'requestPassword',
     'requestURL',
     'plainHMACKeyForSignature',
     'apiKey'].each { String p ->
        if (!message.getProperty(p)) { message.setProperty(p, 'placeholder') }
    }
}

/* ----------------------------------------------------------------------------
 *  generateJwtToken – Header & Claim erzeugen und Base64URL-enkodieren
 * ------------------------------------------------------------------------- */
String generateJwtToken(String apiKey) {
    Map headerMap = [alg: 'RS256', typ: 'JWT', kid: apiKey]
    String header = base64Url(JsonOutput.toJson(headerMap))

    Map claimMap  = [iat: (System.currentTimeMillis() / 1000L).longValue()]
    String claim  = base64Url(JsonOutput.toJson(claimMap))

    return "${header}.${claim}"
}

/* ----------------------------------------------------------------------------
 *  signToken – JWT per HMAC-SHA256 signieren und Base64URL-enkodieren
 * ------------------------------------------------------------------------- */
String signToken(String token, String plainKey) {
    SecretKeySpec keySpec = new SecretKeySpec(plainKey.getBytes('UTF-8'), 'HmacSHA256')
    Mac mac = Mac.getInstance('HmacSHA256')
    mac.init(keySpec)
    byte[] rawHmac = mac.doFinal(token.getBytes('UTF-8'))
    return base64Url(rawHmac)
}

/* ----------------------------------------------------------------------------
 *  callGetAccount – GET /{role}/{UID}
 * ------------------------------------------------------------------------- */
Map callGetAccount(Message msg, String jwt, String sig,
                   String uid, String role, def log) {

    String urlStr = "${msg.getProperty('requestURL')}/${role}/${uid}"
    HttpURLConnection con = (HttpURLConnection) new URL(urlStr).openConnection()
    con.setRequestMethod('GET')
    con.setRequestProperty('Authorization', buildBasicAuth(msg))
    con.setRequestProperty('JWT-Token',    jwt)
    con.setRequestProperty('CDCSignature', sig)
    con.setConnectTimeout(10000)
    con.setReadTimeout(10000)

    int rc = con.responseCode
    log?.addAttachmentAsString('GET-Account-ResponseCode', rc.toString(), 'text/plain')

    if (rc == 200) {
        String jsonTxt = con.inputStream.getText('UTF-8')
        log?.addAttachmentAsString('GET-Account-Body', jsonTxt, 'application/json')
        return extractProfile(new JsonSlurper().parseText(jsonTxt))
    }
    if (rc == 404) {
        return null          // Profil existiert nicht
    }
    throw new RuntimeException("GET Account fehlgeschlagen – HTTP ${rc}")
}

/* ----------------------------------------------------------------------------
 *  buildIncomingProfile – XML → Map
 * ------------------------------------------------------------------------- */
Map buildIncomingProfile(def xml) {
    return [
        firstName : xml.BusinessPartner.Name.FirstName.text(),
        lastName  : xml.BusinessPartner.Name.LastName.text(),
        city      : xml.BusinessPartner.Address.City.text()
    ].findAll { it.value }   // Null-Werte entfernen
}

/* ----------------------------------------------------------------------------
 *  profilesEqual – einfacher Map-Vergleich
 * ------------------------------------------------------------------------- */
boolean profilesEqual(Map p1, Map p2) {
    return (p1 ?: [:]) == (p2 ?: [:])
}

/* ----------------------------------------------------------------------------
 *  callUpdateProfile – POST /{UID}
 * ------------------------------------------------------------------------- */
void callUpdateProfile(Message msg, String jwt, String sig,
                       String uid, Map profile, def log) {

    String urlStr = "${msg.getProperty('requestURL')}/${uid}"
    HttpURLConnection con = (HttpURLConnection) new URL(urlStr).openConnection()
    con.setRequestMethod('POST')
    con.setDoOutput(true)
    con.setRequestProperty('Content-Type', 'application/x-www-form-urlencoded')
    con.setRequestProperty('Authorization', buildBasicAuth(msg))
    con.setRequestProperty('JWT-Token',    jwt)
    con.setRequestProperty('CDCSignature', sig)

    String payload = buildPostBody(msg.getProperty('apiKey'), uid, profile)
    con.outputStream.withWriter('UTF-8') { it << payload }

    int rc = con.responseCode
    log?.addAttachmentAsString('POST-Update-ResponseCode', rc.toString(), 'text/plain')
    if (rc < 200 || rc >= 300) {
        String errBody = con.errorStream?.getText('UTF-8')
        throw new RuntimeException("Profil-Update fehlgeschlagen – HTTP ${rc} – ${errBody}")
    }
}

/* ----------------------------------------------------------------------------
 *  buildPostBody – apiKey=...&UID=...&profile=...
 * ------------------------------------------------------------------------- */
String buildPostBody(String apiKey, String uid, Map profile) {
    String encodedProfile = URLEncoder.encode(JsonOutput.toJson(profile), 'UTF-8')
    return "apiKey=${URLEncoder.encode(apiKey, 'UTF-8')}" +
           "&UID=${URLEncoder.encode(uid, 'UTF-8')}" +
           "&profile=${encodedProfile}"
}

/* ----------------------------------------------------------------------------
 *  extractProfile – CDC-Response → reines Profilobjekt
 * ------------------------------------------------------------------------- */
Map extractProfile(Object jsonResp) {
    if (!jsonResp) return null
    if (jsonResp instanceof Map && jsonResp.profile) {
        return jsonResp.profile as Map
    }
    if (jsonResp instanceof Map && jsonResp.results instanceof List) {
        def hit = jsonResp.results.find { it instanceof Map && it.profile }
        return hit?.profile as Map
    }
    return null
}

/* ----------------------------------------------------------------------------
 *  buildBasicAuth – Authorization Header
 * ------------------------------------------------------------------------- */
String buildBasicAuth(Message msg) {
    String usr = msg.getProperty('requestUser')
    String pwd = msg.getProperty('requestPassword')
    String token = "${usr}:${pwd}".bytes.encodeBase64().toString()
    return "Basic ${token}"
}

/* ----------------------------------------------------------------------------
 *  base64Url – Hilfsfunktion Base64URL-Kodierung (ohne Padding)
 * ------------------------------------------------------------------------- */
String base64Url(String str) {
    base64Url(str.getBytes('UTF-8'))
}
String base64Url(byte[] bytes) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
}

/* ----------------------------------------------------------------------------
 *  handleError – zentrales Error-Handling
 * ------------------------------------------------------------------------- */
void handleError(String body, Exception e, def messageLog) {
    /* Payload als Attachment anhängen, damit dieser im Monitoring verfügbar ist */
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String errMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}