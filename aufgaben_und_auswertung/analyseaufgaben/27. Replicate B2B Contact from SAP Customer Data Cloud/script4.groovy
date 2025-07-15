/******************************************************************************
* Groovy-Skript : CDC-->MDM B2B Profilreplikation (SAP CI)
* Autor         : AI – Senior-Integration-Entwickler
* Version       : 1.0
******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.net.HttpURLConnection
import java.net.URL

/* ============================================================================
 * Einstiegspunkt
 * ==========================================================================*/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {

        /* 1. Header & Property-Vorbereitung */
        Map<String,String> ctx = prepareContext(message, messageLog)

        /* 2. Eingehenden Event lesen (Splitter liefert genau EIN Objekt) */
        def bodyString = message.getBody(String) ?: ''
        def eventJson  = new JsonSlurper().parseText(bodyString)
        def eventObj   = (eventJson?.events instanceof List) ? eventJson.events[0] : eventJson
        String profileID = eventObj?.data?.uid ?: ''
        if (!profileID) { throw new IllegalStateException("UID (profileID) im Event nicht vorhanden.") }
        message.setProperty('profileID', profileID)

        /* 3. JWT-Token und Signatur erzeugen */
        String jwtToken   = createJwtToken(ctx.apiKey)
        String signature  = signData(jwtToken, ctx.plainHMACKeyForSignature)

        /* 4. CDC Konto abrufen */
        String accountResponse = getAccount(ctx, profileID, jwtToken, signature, messageLog)

        /* 5. Validieren */
        Map accountPayload = new JsonSlurper().parseText(accountResponse)
        validateProfile(accountPayload)

        /* 6. Mapping JSON -> XML */
        String xmlPayload = mapResponseToXml(accountPayload)

        /* 7. Replikation an MDM */
        replicateProfile(ctx, profileID, xmlPayload, messageLog)

        /* 8. Rückgabe des XML für nachfolgende Schritte (optional) */
        message.setBody(xmlPayload)
        return message

    } catch (Exception e) {
        /* zentrales Error-Handling */
        handleError(message.getBody(String) as String, e, messageLog)
        return message   // wird nicht erreicht, handleError wirft Exception
    }
}

/* ============================================================================
 *  Hilfsfunktionen
 * ==========================================================================*/

/* ----------------------------------------------------------
 * prepareContext
 * -------------------------------------------------------- */
private Map<String,String> prepareContext(Message msg, def messageLog) {
    /* Liest vorhandene Properties – sofern nicht vorhanden, mit 'placeholder' füllen */
    Map<String,String> ctx = [:]
    [  'requestUser',
       'requestPassword',
       'requestURL',
       'requestURLMDM',
       'requestUserMDM',
       'requestPasswordMDM',
       'plainHMACKeyForSignature',
       'apiKey'
    ].each { String key ->
        def val = msg.getProperty(key) ?: 'placeholder'
        msg.setProperty(key, val)
        ctx[key] = val as String
    }
    messageLog?.addAttachmentAsString('ContextValues', JsonOutput.toJson(ctx), 'application/json')
    return ctx
}

/* ----------------------------------------------------------
 * createJwtToken
 * -------------------------------------------------------- */
private String createJwtToken(String apiKey) {
    /*  Header & Claim zusammenbauen und Base64Url-encodieren                  */
    def headerJson = JsonOutput.toJson([alg:'RS256', typ:'JWT', kid:apiKey])
    def timestamp  = (System.currentTimeMillis() / 1000L).longValue()
    def claimJson  = JsonOutput.toJson([iat:timestamp])
    Base64.Encoder urlEncoder = Base64.getUrlEncoder().withoutPadding()
    String encodedHeader = urlEncoder.encodeToString(headerJson.getBytes(StandardCharsets.UTF_8))
    String encodedClaim  = urlEncoder.encodeToString(claimJson .getBytes(StandardCharsets.UTF_8))
    return "${encodedHeader}.${encodedClaim}"
}

/* ----------------------------------------------------------
 * signData  (HMAC-SHA256)
 * -------------------------------------------------------- */
private String signData(String data, String plainKey) {
    Mac mac = Mac.getInstance("HmacSHA256")
    SecretKeySpec keySpec = new SecretKeySpec(plainKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256")
    mac.init(keySpec)
    byte[] rawHmac = mac.doFinal(data.getBytes(StandardCharsets.UTF_8))
    return Base64.getUrlEncoder().withoutPadding().encodeToString(rawHmac)
}

/* ----------------------------------------------------------
 * getAccount  (CDC GET)
 * -------------------------------------------------------- */
private String getAccount(Map ctx, String profileID, String jwt, String sig, def log) {

    String urlStr = "${ctx.requestURL}/${profileID}"
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.with {
        requestMethod             = 'GET'
        setRequestProperty('Authorization', 'Basic ' + basicAuth(ctx.requestUser, ctx.requestPassword))
        setRequestProperty('JWT-Token'   , jwt)
        setRequestProperty('CDCSignature', sig)
        setRequestProperty('Accept', 'application/json')
        connectTimeout            = 15000
        readTimeout               = 15000
    }

    int rc = conn.responseCode
    String resp = rc < 400 ? conn.inputStream.getText('UTF-8')
                           : conn.errorStream?.getText('UTF-8') ?: ''
    log?.addAttachmentAsString('CDC_GetAccount_Response', resp, 'application/json')

    if (rc != 200) {
        throw new RuntimeException("CDC GetAccount fehlgeschlagen – HTTP ${rc}")
    }
    return resp
}

/* ----------------------------------------------------------
 * validateProfile
 * -------------------------------------------------------- */
private void validateProfile(Map payload) {
    if (!payload.isRegistered) {
        throw new IllegalArgumentException("Validierung fehlgeschlagen: isRegistered != true")
    }
    if (!payload.profile) {
        throw new IllegalArgumentException("Validierung fehlgeschlagen: profile-Objekt fehlt")
    }
    if (!payload.profile.lastName) {
        throw new IllegalArgumentException("Validierung fehlgeschlagen: profile.lastName fehlt")
    }
}

/* ----------------------------------------------------------
 * mapResponseToXml
 * -------------------------------------------------------- */
private String mapResponseToXml(Map payload) {

    String uuid            = UUID.randomUUID().toString()
    String idWithoutDashes = uuid.replaceAll('-', '')

    def writer = new StringWriter()
    def xml    = new MarkupBuilder(writer)
    xml.setDoubleQuotes(true)

    xml.'bp:BusinessPartnerSUITEBulkReplicateRequest'('xmlns:bp':'urn:sap-com:document:sap:rfc:functions') {
        'BusinessPartnerSUITEReplicateRequestMessage' {
            'MessageHeader' {
                'UUID'(uuid)
                'ID'(idWithoutDashes)
            }
            'BusinessPartner' {
                'Common' {
                    'KeyWordsText'(payload.profile.firstName)
                    'AdditionalKeyWordsText'(payload.profile.lastName)
                    'Person' {
                        'Name' {
                            'GivenName'(payload.profile.firstName)
                            'FamilyName'(payload.profile.lastName)
                        }
                    }
                }
                'AddressIndependentCommInfo' {
                    def verifiedEmails = payload.emails?.verified ?: []
                    verifiedEmails.each { emailObj ->
                        if (emailObj?.email) {
                            'Email' {
                                'URI'(emailObj.email)
                            }
                        }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/* ----------------------------------------------------------
 * replicateProfile  (MDM POST)
 * -------------------------------------------------------- */
private void replicateProfile(Map ctx, String profileID, String xmlPayload, def log) {

    String urlStr = "${ctx.requestURLMDM}/${profileID}"
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.with {
        doOutput                    = true
        requestMethod               = 'POST'
        setRequestProperty('Authorization', 'Basic ' + basicAuth(ctx.requestUserMDM, ctx.requestPasswordMDM))
        setRequestProperty('Content-Type' , 'application/xml; charset=UTF-8')
        setRequestProperty('Accept', 'application/xml')
        connectTimeout              = 15000
        readTimeout                 = 15000
    }

    conn.outputStream.withWriter('UTF-8') { it << xmlPayload }
    int rc = conn.responseCode
    String resp = rc < 400 ? conn.inputStream?.getText('UTF-8') : conn.errorStream?.getText('UTF-8')
    log?.addAttachmentAsString('MDM_ReplicateProfile_Response', resp ?: '', 'text/plain')

    if (rc < 200 || rc >= 300) {
        throw new RuntimeException("MDM Replicate fehlgeschlagen – HTTP ${rc}")
    }
}

/* ----------------------------------------------------------
 * basicAuth (Hilfs-Encoder)
 * -------------------------------------------------------- */
private String basicAuth(String user, String pwd) {
    return Base64.getEncoder().encodeToString("${user}:${pwd}".getBytes(StandardCharsets.UTF_8))
}

/* ----------------------------------------------------------
 * handleError  (zentral, gem. Vorgabe)
 * -------------------------------------------------------- */
private void handleError(String body, Exception e, def messageLog) {
    /* Payload als Attachment für Monitoring */
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: '', "text/plain")
    String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}