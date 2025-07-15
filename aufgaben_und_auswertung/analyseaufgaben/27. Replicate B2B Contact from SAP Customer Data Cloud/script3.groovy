/********************************************************************************************
 *  SAP Cloud Integration – Groovy-Script
 *
 *  Ziel:    •   Lesen eines Event-Objekts (Splitter-Schritt) aus der SAP Customer Data Cloud
 *           •   Aufbau eines JWT-Tokens inkl. HMAC-Signatur
 *           •   Aufruf „Get Account“ (CDC) → Profil-JSON
 *           •   Validierung der Profil-Daten
 *           •   Mapping Profil-JSON → BusinessPartnerSUITEBulkReplicateRequest-XML
 *           •   Aufruf „Replicate Profile to MDM“
 *
 *  Autor:   (Senior Integration Developer)
 *******************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.UUID

//---------------------------------------------------------
//  MAIN
//---------------------------------------------------------
Message processData(Message message) {

    /* ----------------------------------------------------
     *  Initialisierung & Error-Handling Wrapper
     * -------------------------------------------------- */
    def messageLog = messageLogFactory.getMessageLog(message)
    try {

        /* 1. Header / Properties in Variablen überführen */
        def env                = readEnvironment(message)

        /* 2. Body (Splitter: 1 Event-Objekt) parsen       */
        def eventJson          = new JsonSlurper().parseText(message.getBody(String) ?: '{}')
        def dataNode           = (eventJson?.data) ?: eventJson                                      // Fallback falls direkt das Datenobjekt ankommt
        env.profileID          = (dataNode?.uid ?: 'placeholder')
        message.setProperty('profileID', env.profileID)

        /* 3. JWT-Token + Signatur aufbauen                */
        def jwtToken           = createJwtToken(env.apiKey)
        def cdcSignature       = signToken(jwtToken, env.hmacKey)

        /* 4. „Get Account“ – CDC API                      */
        def profilePayload     = callGetAccount(env, jwtToken, cdcSignature, messageLog)

        /* 5. Validierung Profil                           */
        validateProfile(profilePayload)                                   // Exception bei Invalidität

        /* 6. Mapping JSON → XML                           */
        String xmlBody         = mapResponseToXml(profilePayload)
        message.setBody(xmlBody)                                          // (für Monitoring)

        /* 7. „Replicate Profile to MDM“ – POST            */
        callPostToMdm(env, xmlBody, messageLog)

        /* 8. Ergebnis in Message zurückgeben              */
        return message

    } catch (Exception e) {
        /* Zentrales Error-Handling */
        return handleError(message, e, messageLog)
    }
}

/* ========================================================================================
 *  FUNKTIONEN
 * ====================================================================================== */

/*---------------------------------------------------------
 *  1) Environment lesen (Properties / Header)
 *-------------------------------------------------------*/
private Map readEnvironment(Message msg) {
    Map<String, String> env = [:]

    env.requestUser        = getValue(msg, 'requestUser')
    env.requestPassword    = getValue(msg, 'requestPassword')
    env.requestURL         = getValue(msg, 'requestURL')
    env.requestURLMDM      = getValue(msg, 'requestURLMDM')
    env.requestUserMDM     = getValue(msg, 'requestUserMDM')
    env.requestPasswordMDM = getValue(msg, 'requestPasswordMDM')
    env.hmacKey            = getValue(msg, 'plainHMACKeyForSignature', 'gekmN8CFr6PngJ9xTGMOMpdTFAvcssg5')
    env.apiKey             = getValue(msg, 'apiKey', 'API_KEY_PROD1')

    return env
}

/*---------------------------------------------------------
 *  2) JWT-Token generieren
 *-------------------------------------------------------*/
private String createJwtToken(String apiKey) {
    def headerJson  = JsonOutput.toJson([alg: 'RS256', typ: 'JWT', kid: apiKey])
    def header      = base64UrlEncode(headerJson)

    long ts         = (System.currentTimeMillis() / 1000L).longValue()
    def claimJson   = JsonOutput.toJson([iat: ts])
    def claim       = base64UrlEncode(claimJson)

    return "${header}.${claim}"
}

/*---------------------------------------------------------
 *  3) HMAC-SHA256 Token signieren
 *-------------------------------------------------------*/
private String signToken(String token, String key) {
    Mac mac       = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
    byte[] rawSig = mac.doFinal(token.getBytes(StandardCharsets.UTF_8))
    return Base64.encoder.encodeToString(rawSig)
}

/*---------------------------------------------------------
 *  4) GET Account (CDC)
 *-------------------------------------------------------*/
private Map callGetAccount(Map env, String jwtToken, String signature, def messageLog) {
    String urlStr = "${env.requestURL}/${env.profileID}"
    def urlCon    = new URL(urlStr).openConnection()
    urlCon.setRequestMethod('GET')
    setBasicAuthHeader(urlCon, env.requestUser, env.requestPassword)
    urlCon.setRequestProperty('JWT-Token',    jwtToken)
    urlCon.setRequestProperty('CDCSignature', signature)

    int rc = urlCon.responseCode
    if (rc != 200) {
        throw new RuntimeException("Get Account – HTTP-Status ${rc} für UID ${env.profileID}")
    }

    String responseJson = urlCon.inputStream.getText('UTF-8')
    messageLog?.addAttachmentAsString("CDC_Response_${env.profileID}", responseJson, "application/json")
    return new JsonSlurper().parseText(responseJson)
}

/*---------------------------------------------------------
 *  5) Validierung des Profils
 *-------------------------------------------------------*/
private void validateProfile(Map payload) {
    if (!payload?.isRegistered) {
        throw new IllegalStateException("Validierung fehlgeschlagen: isRegistered ≠ true")
    }
    if (!payload?.profile) {
        throw new IllegalStateException("Validierung fehlgeschlagen: profile fehlt")
    }
    if (!payload?.profile?.lastName) {
        throw new IllegalStateException("Validierung fehlgeschlagen: profile.lastName fehlt")
    }
}

/*---------------------------------------------------------
 *  6) Mapping JSON → XML (String)
 *-------------------------------------------------------*/
private String mapResponseToXml(Map payload) {
    def uuid       = UUID.randomUUID().toString()
    def idNoDash   = uuid.replaceAll('-', '')

    def writer     = new StringWriter()
    def bp         = new MarkupBuilder(writer)
    bp.bp__BusinessPartnerSUITEBulkReplicateRequest('xmlns:bp':'urn:sap-com:document:sap:rfc:functions') {
        BusinessPartnerSUITEReplicateRequestMessage {
            MessageHeader {
                UUID(uuid)
                ID(idNoDash)
            }
            BusinessPartner {
                Common {
                    KeyWordsText(payload.profile.firstName ?: '')
                    AdditionalKeyWordsText(payload.profile.lastName ?: '')
                    Person {
                        Name {
                            GivenName(payload.profile.firstName ?: '')
                            FamilyName(payload.profile.lastName ?: '')
                        }
                    }
                }
                AddressIndependentCommInfo {
                    if (payload?.emails?.verified instanceof List) {
                        payload.emails.verified.each { mailObj ->
                            Email {
                                URI(mailObj.email ?: '')
                            }
                        }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/*---------------------------------------------------------
 *  7) POST to MDM
 *-------------------------------------------------------*/
private void callPostToMdm(Map env, String xmlBody, def messageLog) {
    String urlStr = "${env.requestURLMDM}/${env.profileID}"
    def urlCon    = new URL(urlStr).openConnection()
    urlCon.setRequestMethod('POST')
    urlCon.doOutput            = true
    urlCon.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
    setBasicAuthHeader(urlCon, env.requestUserMDM, env.requestPasswordMDM)

    urlCon.outputStream.withWriter('UTF-8') { it << xmlBody }
    int rc = urlCon.responseCode

    messageLog?.addAttachmentAsString("MDM_Request_${env.profileID}", xmlBody, "application/xml")
    messageLog?.setStringProperty("MDM_HTTP_Status", rc.toString())

    if (rc >= 300) {
        String err = urlCon.errorStream?.getText('UTF-8') ?: ''
        throw new RuntimeException("Replicate Profile – HTTP-Status ${rc}. Fehler: ${err}")
    }
}

/*---------------------------------------------------------
 *  8) Hilfs-Funktionen
 *-------------------------------------------------------*/
private static String base64UrlEncode(String src) {
    return Base64.getUrlEncoder().withoutPadding()
                 .encodeToString(src.getBytes(StandardCharsets.UTF_8))
}

private static void setBasicAuthHeader(URLConnection con, String user, String pwd) {
    String raw = "${user}:${pwd}"
    String enc = Base64.encoder.encodeToString(raw.getBytes(StandardCharsets.UTF_8))
    con.setRequestProperty('Authorization', "Basic ${enc}")
}

/*---------------------------------------------------------
 *  9) Property/Header generisch lesen
 *-------------------------------------------------------*/
private static String getValue(Message msg, String key, String dflt = 'placeholder') {
    def val = (msg.getProperty(key) ?: msg.getHeader(key, String.class)) ?: dflt
    return val.toString()
}

/*---------------------------------------------------------
 *  10) Zentrales Error-Handling
 *-------------------------------------------------------*/
private Message handleError(Message message, Exception e, def messageLog) {
    // Payload als Attachment
    def body = message.getBody(String) ?: ''
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/plain")
    // Logging & Exception-Weitergabe
    def errMsg = "Fehler im Groovy-Skript: ${e.message}"
    messageLog?.setStringProperty("GroovyError", errMsg)
    throw new RuntimeException(errMsg, e)
}