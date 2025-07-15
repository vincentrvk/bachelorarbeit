/****************************************************************************************
* SAP Cloud Integration – Groovy-Script                                                  *
* Skriptname :  CDC_B2C_2_MDM_Replication.groovy                                         *
* Zweck      :  Repliziert CDC-B2C-Profile in das SAP MDM                                *
* Autor      :  ChatGPT (generierter Beispiel-Code)                                      *
****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets

//=====================================================================
// Haupt­einstieg
//=====================================================================
Message processData(Message message) {

    def body        = message.getBody(String) ?: ''
    def messageLog  = messageLogFactory.getMessageLog(message)

    try {

        /* -------------------------------------------------------------------
         * 0.- Initiale Bereitstellung von Headern & Properties
         * ------------------------------------------------------------------*/
        setInitialValues(message)

        /* -------------------------------------------------------------------
         * 1.- Split & Verarbeitung der Event-Liste
         * ------------------------------------------------------------------*/
        JsonSlurper slurper = new JsonSlurper()
        def jsonIn  = slurper.parseText(body)
        def events  = jsonIn?.events ?: []

        events.each { eventObject ->

            /* 1.1 Event-Validierung */
            validateEvent(eventObject)

            /* 1.2 Profil- und Account-Typen auslesen */
            message.setProperty('profileID',   eventObject?.data?.uid        ?: '')
            message.setProperty('accountType', eventObject?.data?.accountType ?: '')

            /* 1.3 JWT-Token & Signature erzeugen                                  */
            def jwtToken    = createJwtToken(message.getProperty('apiKey'))
            def cdcSig      = createCdcSignature(jwtToken,
                                                 message.getProperty('plainHMACKeyForSignature'))

            /* 1.4 GET-Account aufrufen                                            */
            def profileJson = callGetAccount(message,
                                             jwtToken,
                                             cdcSig)

            /* 1.5 Profil-Validierung                                             */
            validateProfile(profileJson)

            /* 1.6 Mapping JSON → XML                                             */
            def xmlString = mapProfileToMdmXml(profileJson)

            /* 1.7 Replikation zum MDM                                            */
            callReplicateProfile(message, xmlString)

            /* 1.8 Letzte erzeugte XML als Message-Body zu Debug-Zwecken          */
            message.setBody(xmlString)
        }

        return message

    } catch(Exception e) {
        handleError(message, body, e)
    }
}

/* =====================================================================================
 * Funktions-Bereich
 * =================================================================================== */

/*-----------------------------------------------------------------------------
 * setInitialValues()
 * Liest benötigte Properties/Headers aus der Message oder befüllt sie mit
 * Default-Platzhaltern, sofern nicht vorhanden.
 *---------------------------------------------------------------------------*/
private void setInitialValues(Message message) {
    ['requestUser',
     'requestPassword',
     'requestURL',
     'requestURLMDM',
     'requestUserMDM',
     'requestPasswordMDM',
     'plainHMACKeyForSignature',
     'apiKey'].each { key ->
        if(!message.getProperty(key)) {
            message.setProperty(key, key == 'plainHMACKeyForSignature' ? 
                                      'gekmN8CFr6PngJ9xTGMOMpdTFAvcssg5' :
                                      'placeholder')
        }
    }
}

/*-----------------------------------------------------------------------------
 * validateEvent()
 * Prüft eingehendes Event auf Einhaltung der Validierungsregeln.
 *---------------------------------------------------------------------------*/
private void validateEvent(def eventObj) {
    def validTypes = ['accountCreated','accountUpdated','accountRegistered']
    if(!validTypes.contains(eventObj?.type)) {
        throw new IllegalArgumentException("Ungültiger Event-Typ: ${eventObj?.type}")
    }
    if(!eventObj?.data?.uid) {
        throw new IllegalArgumentException("UID im Event fehlt.")
    }
}

/*-----------------------------------------------------------------------------
 * createJwtToken()
 * Erstellt den JWT-Token gem. Vorgabe (Header + Claim, Base64-URL-encoded).
 *---------------------------------------------------------------------------*/
private String createJwtToken(String apiKey) {

    def headerJson  = JsonOutput.toJson([alg:'RS256',typ:'JWT',kid:apiKey])
    def headerB64   = base64UrlEncode(headerJson)

    long now        = (System.currentTimeMillis() / 1000L).longValue()
    def claimB64    = base64UrlEncode(now.toString())

    return "${headerB64}.${claimB64}"
}

/*-----------------------------------------------------------------------------
 * createCdcSignature()
 * Signiert den JWT-Token mittels HMAC-SHA256 und liefert Base64-String zurück.
 *---------------------------------------------------------------------------*/
private String createCdcSignature(String token, String plainKey) {

    Mac mac  = Mac.getInstance('HmacSHA256')
    mac.init(new SecretKeySpec(plainKey.getBytes(StandardCharsets.UTF_8), 'HmacSHA256'))

    byte[] rawHmac = mac.doFinal(token.getBytes(StandardCharsets.UTF_8))
    return Base64.encoder.encodeToString(rawHmac)
}

/*-----------------------------------------------------------------------------
 * callGetAccount()
 * Ruft CDC-API auf, übergibt JWT & Signature und liefert das Profil-JSON.
 *---------------------------------------------------------------------------*/
private Map callGetAccount(Message message,
                           String jwtToken,
                           String cdcSig) {

    String baseUrl     = message.getProperty('requestURL')
    String profileID   = message.getProperty('profileID')
    String accountType = message.getProperty('accountType')

    String user        = message.getProperty('requestUser')
    String pwd         = message.getProperty('requestPassword')
    String basicAuth   = Base64.encoder.encodeToString("${user}:${pwd}".bytes)

    String endpoint    = "${baseUrl}/${accountType}/${profileID}"
    URL url            = new URL(endpoint)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()

    conn.setRequestMethod('GET')
    conn.setRequestProperty('Authorization', "Basic ${basicAuth}")
    conn.setRequestProperty('JWT-Token',      jwtToken)
    conn.setRequestProperty('CDCSignature',   cdcSig)
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(15000)

    int rc = conn.responseCode
    if(rc != HttpURLConnection.HTTP_OK) {
        throw new RuntimeException("GET-Account Fehler: HTTP-Status ${rc}")
    }

    def jsonResp = conn.inputStream.getText('UTF-8')
    return new JsonSlurper().parseText(jsonResp)
}

/*-----------------------------------------------------------------------------
 * validateProfile()
 * Stellt sicher, dass ein Profil mit Nachnamen vorhanden ist.
 *---------------------------------------------------------------------------*/
private void validateProfile(Map json) {
    if(!json?.profile)          { throw new IllegalArgumentException("Profil fehlt in Response") }
    if(!json.profile.lastName)  { throw new IllegalArgumentException("Nachname im Profil fehlt") }
}

/*-----------------------------------------------------------------------------
 * mapProfileToMdmXml()
 * Erstellt die MDM-XML gemäß Mapping-Vorgabe und liefert als String zurück.
 *---------------------------------------------------------------------------*/
private String mapProfileToMdmXml(Map json) {

    def profile = json.profile
    def emails  = json?.emails?.verified ?: []

    String uuid = UUID.randomUUID().toString()
    String id   = uuid.replaceAll('-', '')

    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)

    xml.'bp:BusinessPartnerSUITEBulkReplicateRequest'(
            'xmlns:bp':'urn:sap-com:document:sap:rfc:functions') {

        BusinessPartnerSUITEReplicateRequestMessage {
            MessageHeader {
                UUID(uuid)
                ID(id)
            }
            BusinessPartner {
                Common {
                    KeyWordsText(profile?.firstName ?: '')
                    AdditionalKeyWordsText(profile?.lastName ?: '')
                    Person {
                        Name {
                            GivenName(profile?.firstName ?: '')
                            FamilyName(profile?.lastName ?: '')
                        }
                    }
                }
                AddressIndependentCommInfo {
                    emails.each { mailObj ->
                        if(mailObj?.email) {
                            Email {
                                URI(mailObj.email)
                            }
                        }
                    }
                }
            }
        }
    }
    return sw.toString()
}

/*-----------------------------------------------------------------------------
 * callReplicateProfile()
 * POSTet die erzeugte XML an das MDM-Zielsystem.
 *---------------------------------------------------------------------------*/
private void callReplicateProfile(Message message,
                                  String xmlPayload) {

    String baseUrl   = message.getProperty('requestURLMDM')
    String profileID = message.getProperty('profileID')
    String endpoint  = "${baseUrl}/${profileID}"

    String user      = message.getProperty('requestUserMDM')
    String pwd       = message.getProperty('requestPasswordMDM')
    String basicAuth = Base64.encoder.encodeToString("${user}:${pwd}".bytes)

    URL url          = new URL(endpoint)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()

    conn.setRequestMethod('POST')
    conn.doOutput             = true
    conn.setRequestProperty('Authorization', "Basic ${basicAuth}")
    conn.setRequestProperty('Content-Type',  'application/xml;charset=UTF-8')

    conn.outputStream.withWriter('UTF-8') { it << xmlPayload }

    int rc = conn.responseCode
    if(rc != HttpURLConnection.HTTP_OK &&
       rc != HttpURLConnection.HTTP_CREATED &&
       rc != HttpURLConnection.HTTP_ACCEPTED) {
        throw new RuntimeException("MDM-Replication Fehler: HTTP-Status ${rc}")
    }
}

/*-----------------------------------------------------------------------------
 * base64UrlEncode()
 * Hilfsroutine: Base64-URL-Encoding ohne Padding.
 *---------------------------------------------------------------------------*/
private String base64UrlEncode(String input) {
    return Base64.getUrlEncoder()
                 .withoutPadding()
                 .encodeToString(input.getBytes(StandardCharsets.UTF_8))
}

/*-----------------------------------------------------------------------------
 * handleError()
 * Zentrales Error-Handling gem. Vorgabe (Attachment + aussagekräftige Log-Msg).
 *---------------------------------------------------------------------------*/
private void handleError(Message message, String payload, Exception e) {
    def messageLog = messageLogFactory.getMessageLog(message)
    messageLog?.addAttachmentAsString('ErrorPayload', payload ?: '', 'text/plain')
    def errorMsg = "Fehler im CDC-MDM-Mapping-Skript: ${e.message}"
    messageLog?.setStringProperty('GroovyError', errorMsg)
    throw new RuntimeException(errorMsg, e)
}