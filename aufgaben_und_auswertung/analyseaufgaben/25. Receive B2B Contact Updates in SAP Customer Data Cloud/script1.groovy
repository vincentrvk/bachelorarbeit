/*************************************************************************************************
*  Groovy Script:  B2B-Contact Update – SAP MDI  -> SAP CDC
*  Description  :  Repliziert BusinessPartner-Änderungen aus SAP Master Data Integration
*                  als Contact-Updates in die SAP Customer Data Cloud.
*  Autor        :  AI Assistant
*  Version      :  1.0 – 2025-06-18
*************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.net.HttpURLConnection
import java.util.Base64

Message processData(Message message) {

    /*-----------------------------------------------------
     * 0. Initialisierung
     *---------------------------------------------------*/
    final MessageLog log  = messageLogFactory?.getMessageLog(message)
    final String     body = message.getBody(String) ?: ''

    try {
        /*-------------------------------------------------
         * 1. Properties & Header befüllen
         *-----------------------------------------------*/
        setDefaults(message)

        /*-------------------------------------------------
         * 2. XML Split & Verarbeitung jedes Partners
         *-----------------------------------------------*/
        def partners = extractPartners(body)
        partners.each { partnerNode ->

            /*------------- 2.1 Profil-ID ----------------*/
            String profileId = partnerNode.Common?.IAMID?.text()
            if (!profileId) {
                throw new IllegalStateException('Keine IAMID im BusinessPartner gefunden.')
            }
            message.setProperty('profileID', profileId)

            /*------------- 2.2 Eingehendes Profil -------*/
            Map incomingProfile = mapIncomingProfile(partnerNode)

            /*------------- 2.3 JWT & Signatur -----------*/
            String jwtToken   = createJwt(message)
            String signature  = signJwt(jwtToken, message)

            /*------------- 2.4 GET Account --------------*/
            String existingJson = callGetAccount(message, jwtToken, signature)
            Map    existingProfile = existingJson ? filterProfile(existingJson) : null

            /*------------- 2.5 Profil-Vergleich ---------*/
            Map diffProfile = compareProfiles(existingProfile, incomingProfile)

            /*------------- 2.6 Kein Delta -> Fehler -----*/
            if (diffProfile.isEmpty()) {
                throw new IllegalStateException("Kein Delta für Profil ${profileId} vorhanden.")
            }

            /*------------- 2.7 POST Update --------------*/
            callUpdateProfile(message, jwtToken, signature, diffProfile)
        }

        /* Erfolgreich – Body unverändert weitergeben */
        return message

    } catch (Exception e) {
        /* Globales Error-Handling */
        handleError(body, e, log)
        return message   // (wird nie erreicht – handleError wirft Exception)
    }
}


/* ==============================================================================================
 *  HELPER-FUNKTIONEN
 * ============================================================================================*/

/*---------------------------------------------
 * setDefaults
 *-------------------------------------------*/
void setDefaults(Message msg) {
    /* Pflicht-Properties & Default-Werte */
    def defaults = [
            requestUser                : 'placeholder',
            requestPassword            : 'placeholder',
            requestURL                 : 'placeholder',
            plainHMACKeyForSignature   : 'gekmN8CFr6PngJ9xTGMOMpdTFAvcssg5',
            apiKey                     : 'API_KEY_PROD1'
    ]
    defaults.each { key, defVal ->
        if (!msg.getProperty(key)) {
            msg.setProperty(key, defVal)
        }
    }
}

/*---------------------------------------------
 * extractPartners
 *-------------------------------------------*/
List extractPartners(String xmlBody) {
    def xml = new XmlSlurper().parseText(xmlBody)
    // Suche nach allen BusinessPartner-Elementen (Namespaces ignorieren)
    return xml.'**'.findAll { it.name().localPart == 'BusinessPartner' }
}

/*---------------------------------------------
 * mapIncomingProfile
 *-------------------------------------------*/
Map mapIncomingProfile(def partner) {
    /* Erstellt das zu importierende Profil-Objekt */
    return [
            firstName : partner?.Name?.FirstName?.text(),
            lastName  : partner?.Name?.LastName?.text(),
            city      : partner?.Address?.CityName?.text()
            /* Bei Bedarf zusätzliche Felder ergänzen */
    ].findAll { k, v -> v }         // entferne Null-Werte
}

/*---------------------------------------------
 * createJwt
 *-------------------------------------------*/
String createJwt(Message msg) {
    String apiKey = msg.getProperty('apiKey') as String
    Map header  = [alg: 'RS256', typ: 'JWT', kid: apiKey]
    Map claim   = [iat: (System.currentTimeMillis() / 1000) as long]

    String encHeader = base64Url(JsonOutput.toJson(header))
    String encClaim  = base64Url(JsonOutput.toJson(claim))

    String jwt = "${encHeader}.${encClaim}"
    msg.setHeader('JWT-Token', jwt)
    return jwt
}

/*---------------------------------------------
 * signJwt
 *-------------------------------------------*/
String signJwt(String jwt, Message msg) {
    String secret    = msg.getProperty('plainHMACKeyForSignature') as String
    SecretKeySpec ks = new SecretKeySpec(secret.bytes, 'HmacSHA256')
    Mac mac          = Mac.getInstance('HmacSHA256')
    mac.init(ks)
    byte[] raw       = mac.doFinal(jwt.bytes)
    String signature = base64Url(raw)
    msg.setHeader('CDCSignature', signature)
    return signature
}

/*---------------------------------------------
 * callGetAccount   (GET)
 *-------------------------------------------*/
String callGetAccount(Message msg, String jwt, String signature) {

    String baseUrl   = msg.getProperty('requestURL')    as String
    String user      = msg.getProperty('requestUser')   as String
    String pass      = msg.getProperty('requestPassword') as String
    String profileId = msg.getProperty('profileID')     as String

    HttpURLConnection conn
    try {
        conn = (HttpURLConnection) new URL("${baseUrl}/${profileId}").openConnection()
        conn.requestMethod = 'GET'
        conn.setConnectTimeout(10000)
        conn.setReadTimeout(20000)

        String auth = "${user}:${pass}".bytes.encodeBase64().toString()
        conn.setRequestProperty('Authorization', "Basic ${auth}")
        conn.setRequestProperty('JWT-Token', jwt)
        conn.setRequestProperty('CDCSignature', signature)

        int rc = conn.responseCode
        if (rc == 200) {
            return conn.inputStream.text
        }
        if (rc == 404) {
            /* Kein Profil vorhanden */
            return null
        }
        throw new RuntimeException("GET Account fehlgeschlagen – HTTP ${rc}: ${conn.errorStream?.text}")

    } finally {
        conn?.disconnect()
    }
}

/*---------------------------------------------
 * filterProfile
 *-------------------------------------------*/
Map filterProfile(String jsonString) {
    def json = new JsonSlurper().parseText(jsonString)
    return json?.profile as Map ?: [:]
}

/*---------------------------------------------
 * compareProfiles
 *-------------------------------------------*/
Map compareProfiles(Map existing, Map incoming) {

    if (existing == null || existing.isEmpty()) {
        return incoming          // kompletter Import, wenn nichts vorhanden
    }

    Map diff = [:]
    incoming.each { k, v ->
        if (v && (!existing.containsKey(k) || existing[k] != v)) {
            diff[k] = v
        }
    }
    return diff
}

/*---------------------------------------------
 * callUpdateProfile   (POST)
 *-------------------------------------------*/
void callUpdateProfile(Message msg, String jwt, String signature, Map diffProfile) {

    String baseUrl   = msg.getProperty('requestURL')    as String
    String user      = msg.getProperty('requestUser')   as String
    String pass      = msg.getProperty('requestPassword') as String
    String apiKey    = msg.getProperty('apiKey')        as String
    String profileId = msg.getProperty('profileID')     as String

    String queryString = new StringBuilder()
            .append('apiKey=').append(URLEncoder.encode(apiKey     , 'UTF-8'))
            .append('&UID='  ).append(URLEncoder.encode(profileId  , 'UTF-8'))
            .append('&profile=').append(URLEncoder.encode(JsonOutput.toJson(diffProfile), 'UTF-8'))
            .toString()

    HttpURLConnection conn
    try {
        conn = (HttpURLConnection) new URL("${baseUrl}/${profileId}").openConnection()
        conn.requestMethod = 'POST'
        conn.setDoOutput(true)
        conn.setConnectTimeout(10000)
        conn.setReadTimeout(20000)

        String auth = "${user}:${pass}".bytes.encodeBase64().toString()
        conn.setRequestProperty('Authorization', "Basic ${auth}")
        conn.setRequestProperty('Content-Type', 'application/x-www-form-urlencoded')
        conn.setRequestProperty('JWT-Token', jwt)
        conn.setRequestProperty('CDCSignature', signature)

        conn.outputStream.withWriter('UTF-8') { it << queryString }

        int rc = conn.responseCode
        if (rc !in [200, 201, 202]) {
            throw new RuntimeException("UPDATE Profil fehlgeschlagen – HTTP ${rc}: ${conn.errorStream?.text}")
        }

    } finally {
        conn?.disconnect()
    }
}

/*---------------------------------------------
 * base64Url  (URL-safe Base64 ohne Padding)
 *-------------------------------------------*/
String base64Url(byte[] input) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(input)
}

String base64Url(String input) {
    return base64Url(input.bytes)
}

/*---------------------------------------------
 * handleError   (global)
 *-------------------------------------------*/
void handleError(String payload, Exception e, MessageLog mLog) {
    mLog?.addAttachmentAsString('ErrorPayload', payload ?: '', 'text/plain')
    String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}