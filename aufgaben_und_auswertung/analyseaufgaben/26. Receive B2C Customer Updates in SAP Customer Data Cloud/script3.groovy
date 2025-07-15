/*************************************************************************
 * Groovy-Skript:  MDI → SAP Customer Data Cloud – B2C Contact Update
 *
 * Dieses Skript ist für den Einsatz in einer CPI-Groovy-Script-Step
 * innerhalb eines Splitters gedacht (d.h. der Body enthält genau ein
 * <BusinessPartner/>-Element). Es erfüllt alle in der Aufgabenstellung
 * geforderten Schritte – inklusive modularer Struktur, Logging und
 * Fehlerbehandlung.
 *************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.util.Base64

/* =======================================================================
 * Entry-Point
 * ===================================================================== */
Message processData(Message message) {

    // Vorbereitung: MessageLog ermitteln
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* ---------------------------------------------------------------
         * 1) Properties & Header setzen bzw. auslesen
         * ------------------------------------------------------------- */
        setHeadersAndProperties(message)

        /* ---------------------------------------------------------------
         * 2) XML auslesen → Profil-ID & Rolle
         * ------------------------------------------------------------- */
        extractIdentifiers(message)

        /* ---------------------------------------------------------------
         * 3) JWT-Token erzeugen
         * ------------------------------------------------------------- */
        createJwtToken(message)

        /* ---------------------------------------------------------------
         * 4) GET Account aufrufen
         * ------------------------------------------------------------- */
        def cdcProfile = callGetAccount(message, messageLog)

        /* ---------------------------------------------------------------
         * 5) Eingehendes Profil (XML) in JSON überführen
         * ------------------------------------------------------------- */
        def incomingProfile = mapIncomingProfileToJson(message)

        /* ---------------------------------------------------------------
         * 6) Profile vergleichen und zu aktualisierende Felder ermitteln
         * ------------------------------------------------------------- */
        def deltaProfile = compareProfiles(incomingProfile, cdcProfile)

        /* Wenn es nichts zu aktualisieren gibt → Fehler werfen           */
        if (deltaProfile == null || deltaProfile.isEmpty()) {
            throw new Exception("Keine Änderungen zum bestehenden Profil gefunden.")
        }

        /* ---------------------------------------------------------------
         * 7) POST Update Profile
         * ------------------------------------------------------------- */
        callUpdateProfile(message, deltaProfile, messageLog)

        /* Erfolgreich – Rückgabe des (unveränderten) Message-Objekts */
        return message
    }
    catch (Exception ex) {
        /* ---------------------------------------------------------------
         * Fehlerbehandlung an einer zentralen Stelle
         * ------------------------------------------------------------- */
        handleError(message, ex, messageLog)
        // handleError wirft bereits Exception → return ist nur Formalität
        return message
    }
}

/* =======================================================================
 *  Hilfs-Funktionen
 * ===================================================================== */

/* -----------------------------------------------------------------------
 * setHeadersAndProperties
 * --------------------------------------------------------------------- */
void setHeadersAndProperties(Message msg) {
    /* Liest vorhandene Properties/Headers oder setzt Default-Placeholder. */
    Map defaults = [
            requestUser     : 'placeholder',
            requestPassword : 'placeholder',
            requestURL      : 'placeholder',
            plainHMACKeyForSignature : 'placeholder',
            apiKey          : 'placeholder'
    ]
    defaults.each { k, v ->
        if (!msg.getProperty(k)) {
            msg.setProperty(k, v)
        }
    }
}

/* -----------------------------------------------------------------------
 * extractIdentifiers
 * --------------------------------------------------------------------- */
void extractIdentifiers(Message msg) {
    /* Extrahiert IAMID (UID) sowie Rolle aus dem BusinessPartner-XML.    */
    def xml = new XmlSlurper().parseText(msg.getBody(String) as String)
    def ns  = new groovy.xml.Namespace("http://sap.com/xi/SAPGlobal20/Global", '')

    String uid   = xml.BusinessPartner.Common.IAMID.text()
    String role  = xml.BusinessPartner.Role.RoleCode.text()

    if (!uid)  throw new Exception("Keine IAMID gefunden!")
    if (!role) throw new Exception("Keine Rolle gefunden!")

    msg.setProperty('profileID', uid)
    msg.setProperty('role', role)
}

/* -----------------------------------------------------------------------
 * createJwtToken
 * --------------------------------------------------------------------- */
void createJwtToken(Message msg) {
    /* Baut einen simplen Header + Claim und speichert JWT in Property.   */
    String apiKey   = msg.getProperty('apiKey')
    long   ts       = (System.currentTimeMillis() / 1000L) as long

    String headerJson = JsonOutput.toJson([alg: 'RS256', typ: 'JWT', kid: apiKey])
    String claimJson  = JsonOutput.toJson([iat: ts])

    String jwt = "${base64UrlEncode(headerJson.bytes)}.${base64UrlEncode(claimJson.bytes)}"
    msg.setProperty('jwtToken', jwt)

    /* Signatur (HMAC-SHA256) */
    String secret = msg.getProperty('plainHMACKeyForSignature')
    Mac mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secret.bytes, "HmacSHA256"))
    byte[] sig = mac.doFinal(jwt.bytes)
    msg.setProperty('cdcSignature', base64UrlEncode(sig))
}

/* -----------------------------------------------------------------------
 * callGetAccount
 * --------------------------------------------------------------------- */
Map callGetAccount(Message msg, def log) {
    /* Führt GET-Aufruf zum Lesen des Profils durch und gibt JSON Map zurück. */
    String baseUrl   = msg.getProperty('requestURL')
    String role      = msg.getProperty('role')
    String uid       = msg.getProperty('profileID')
    String user      = msg.getProperty('requestUser')
    String pwd       = msg.getProperty('requestPassword')
    String jwt       = msg.getProperty('jwtToken')
    String sig       = msg.getProperty('cdcSignature')

    String urlStr = "${baseUrl}/${role}/${uid}"
    def conn = new URL(urlStr).openConnection()
    String basicAuth = "${user}:${pwd}".bytes.encodeBase64().toString()

    conn.setRequestProperty('Authorization', "Basic ${basicAuth}")
    conn.setRequestProperty('JWT-Token', jwt)
    conn.setRequestProperty('CDCSignature', sig)
    conn.setRequestMethod('GET')
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(15000)

    int rc = conn.responseCode
    if (rc == 200) {
        def responseText = conn.inputStream.getText(StandardCharsets.UTF_8.name())
        log?.addAttachmentAsString('CDC_GET_Response', responseText, 'application/json')
        def json = new JsonSlurper().parseText(responseText)
        /* Wenn Prospect-Response Struktur ⇒ nur "results[*].profile" zusammenführen */
        if (json?.results) {
            def profiles = json.results.findAll { it.profile }.collect { it.profile }
            return profiles ? profiles[0] : [:]
        }
        /* Customer-Response Struktur ⇒ direkt profile-Objekt */
        return json?.profile ?: [:]
    }
    else if (rc == 404) {
        /* Profil existiert nicht → leere Map zurückgeben */
        return [:]
    }
    else {
        throw new Exception("GET Account HTTP-Status: ${rc}")
    }
}

/* -----------------------------------------------------------------------
 * mapIncomingProfileToJson
 * --------------------------------------------------------------------- */
Map mapIncomingProfileToJson(Message msg) {
    /* Wandelt die relevanten Felder des XML in eine Map um.              */
    def xml = new XmlSlurper().parseText(msg.getBody(String) as String)
    def ns  = new groovy.xml.Namespace("http://sap.com/xi/SAPGlobal20/Global", '')

    return [
            firstName : xml.BusinessPartner.Name.FirstName.text(),
            lastName  : xml.BusinessPartner.Name.LastName.text(),
            city      : xml.BusinessPartner.Address.City.text()
    ].findAll { it.value }        // entfernt Null/Leer-Werte
}

/* -----------------------------------------------------------------------
 * compareProfiles
 * --------------------------------------------------------------------- */
Map compareProfiles(Map incoming, Map existing) {
    /* Erstellt eine Map mit nur den Feldern, die sich geändert haben.    */
    Map delta = [:]
    incoming.each { k, v ->
        if (!existing?.containsKey(k) || existing[k] != v) {
            delta[k] = v
        }
    }
    return delta
}

/* -----------------------------------------------------------------------
 * callUpdateProfile
 * --------------------------------------------------------------------- */
void callUpdateProfile(Message msg, Map deltaProfile, def log) {
    /* Sendet POST-Aufruf zur Aktualisierung des Profils.                 */
    String baseUrl   = msg.getProperty('requestURL')
    String uid       = msg.getProperty('profileID')
    String user      = msg.getProperty('requestUser')
    String pwd       = msg.getProperty('requestPassword')
    String jwt       = msg.getProperty('jwtToken')
    String sig       = msg.getProperty('cdcSignature')
    String apiKey    = msg.getProperty('apiKey')

    String profileJson = JsonOutput.toJson(deltaProfile)
    String payload     =
            "apiKey=${URLEncoder.encode(apiKey, 'UTF-8')}" +
            "&UID=${URLEncoder.encode(uid, 'UTF-8')}" +
            "&profile=${URLEncoder.encode(profileJson, 'UTF-8')}"

    String urlStr = "${baseUrl}/${uid}"
    def conn = new URL(urlStr).openConnection()
    String basicAuth = "${user}:${pwd}".bytes.encodeBase64().toString()

    conn.setDoOutput(true)
    conn.setRequestMethod('POST')
    conn.setRequestProperty('Content-Type', 'application/x-www-form-urlencoded')
    conn.setRequestProperty('Authorization', "Basic ${basicAuth}")
    conn.setRequestProperty('JWT-Token', jwt)
    conn.setRequestProperty('CDCSignature', sig)

    conn.outputStream.withWriter { it << payload }
    int rc = conn.responseCode

    if (rc in [200, 201, 202, 204]) {
        log?.addAttachmentAsString('CDC_POST_Payload', payload, 'text/plain')
    } else {
        throw new Exception("POST Update Profile HTTP-Status: ${rc}")
    }
}

/* -----------------------------------------------------------------------
 * handleError
 * --------------------------------------------------------------------- */
void handleError(Message msg, Exception e, def log) {
    /* Zentrales Error-Handling: Payload als Attachment + aussagekräftige
     * RuntimeException werfen, damit IFlow in Fehler läuft.              */
    String body = msg.getBody(String) as String
    log?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    log?.addAttachmentAsString('Exception', e.toString(), 'text/plain')
    throw new RuntimeException("Fehler im MDI→CDC-Skript: ${e.message}", e)
}

/* -----------------------------------------------------------------------
 * base64UrlEncode
 * --------------------------------------------------------------------- */
String base64UrlEncode(byte[] data) {
    /* Kodiert Bytearray URL-safe ohne Padding gemäß JWT-Spezifikation.   */
    return Base64.getUrlEncoder().withoutPadding().encodeToString(data)
}