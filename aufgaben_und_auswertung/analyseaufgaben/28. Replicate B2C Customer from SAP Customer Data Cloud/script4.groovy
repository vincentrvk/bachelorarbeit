/****************************************************************************************
 *  SAP Cloud Integration – Groovy-Skript
 *  Aufgabe:  CDC-Profile validieren, abrufen, mappen und an MDM replizieren
 *
 *  Autor:    Senior-Developer – Integrationen & Groovy
 *  Version:  1.0
 *
 *  Hinweis:
 *  • Das Skript ist für einen Einsatz NACH einem JSON-Splitter gedacht, d. h. es verarbeitet
 *    immer genau EIN „event“-Objekt pro Aufruf.
 *  • Werden Werte (Header/Properties) bereits von vorgelagerten Schritten gesetzt, so
 *    übernimmt das Skript diese. Fehlen sie, wird „placeholder“ verwendet.
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.xml.MarkupBuilder
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.util.Base64
import java.nio.charset.StandardCharsets

/* ================================================================================ */
/*                              H A U P T P R O Z E S S                             */
/* ================================================================================ */
Message processData(Message message) {

    /* -------------------------------------------------------------------- *
     *   Initialisierung
     * -------------------------------------------------------------------- */
    def body        = message.getBody(String) ?: ''
    def msgLog      = messageLogFactory.getMessageLog(message)

    try {

        /* 1. Header & Property-Vorbereitung */
        def ctx = prepareContext(message)

        /* 2. Event / Eingangs-JSON parsen & validieren */
        def event = parseAndValidateEvent(body, msgLog)

        /* 3. Profil-UID & AccountType als Properties setzen */
        message.setProperty('profileID',   event.data.uid)
        message.setProperty('accountType', event.data.accountType)

        /* 4. JWT-Token + CDC-Signature erzeugen & als Header setzen */
        def jwtToken      = buildJwtToken(ctx.apiKey)
        def cdcSignature  = buildSignature(jwtToken, ctx.hmacKey)
        message.setHeader('JWT-Token',     jwtToken)
        message.setHeader('CDCSignature',  cdcSignature)

        /* 5. Account aus CDC abrufen */
        def profileObject = callGetAccount(
                                ctx.requestURL,
                                event.data.accountType,
                                event.data.uid,
                                ctx.requestUser,
                                ctx.requestPassword,
                                jwtToken,
                                cdcSignature,
                                msgLog)

        /* 6. Profil-Validierung */
        validateProfile(profileObject, msgLog)

        /* 7. Mapping: JSON-> XML für MDM   */
        def xmlPayload = mapProfileToMdmXml(profileObject)
        message.setBody(xmlPayload)

        /* 8. Technische Header für den nachgelagerten HTTP-Empfänger */
        def basicAuth = "${ctx.requestUserMDM}:${ctx.requestPasswordMDM}"
                            .bytes.encodeBase64().toString()
        message.setHeader('Authorization', "Basic ${basicAuth}")
        message.setHeader('Content-Type',  'application/xml;charset=UTF-8')
        message.setHeader('Accept',        'application/xml')

        /* 9. Ziel-URL als Property – wird im HTTP-Receiver verwendet       */
        message.setProperty('targetURL',
                "${ctx.requestURLMDM}/${event.data.uid}")

        return message

    } catch (Exception ex) {
        /* zentrales Error-Handling */
        handleError(body, ex, msgLog)
        /* handleError wirft RuntimeException, return ist unreachable       */
    }
}

/* ================================================================================ */
/*                                  F U N K T I O N E N                             */
/* ================================================================================ */

/* -----------------------  prepareContext  --------------------------------------- */
/* Liest benötigte Header & Properties oder setzt „placeholder“,                   */
/* fasst das Ergebnis in einer Map zusammen und liefert diese zurück               */
private Map prepareContext(Message message) {

    def getProp    = { key -> (message.getProperty(key) ?: 'placeholder') as String }
    def getHeader  = { key -> (message.getHeader(key, String) ?: 'placeholder') }

    return [
        requestUser       : getProp('requestUser'),
        requestPassword   : getProp('requestPassword'),
        requestURL        : getProp('requestURL'),
        requestURLMDM     : getProp('requestURLMDM'),
        requestUserMDM    : getProp('requestUserMDM'),
        requestPasswordMDM: getProp('requestPasswordMDM'),
        hmacKey           : getProp('plainHMACKeyForSignature'),
        apiKey            : getProp('apiKey')
    ]
}

/* -----------------------  parseAndValidateEvent  -------------------------------- */
/* Parst den Body als JSON, prüft Event-Typ und liefert das Event-Objekt zurück    */
private Map parseAndValidateEvent(String body, def msgLog) {

    def json = new JsonSlurper().parseText(body)

    /* Falls der Splitter noch nicht ausgeführt wurde, das erste Event nehmen      */
    def event = (json instanceof Map && json.events) ? json.events[0] : json

    /* Validation                                                                  */
    if (!event?.type || !['accountCreated','accountUpdated','accountRegistered']
            .contains(event.type)) {
        throw new IllegalArgumentException(
            "Ungültiger Event-Typ: ${event?.type ?: 'null'}")
    }
    if (!event?.data?.uid) {
        throw new IllegalArgumentException('UID im Event fehlt')
    }
    return event
}

/* -----------------------  buildJwtToken  --------------------------------------- */
/* Baut einen kompakten JWT-Token:  base64url(header).base64url(claim)             */
private String buildJwtToken(String apiKey) {

    def header = [alg:'RS256', typ:'JWT', kid: apiKey]
    def claim  = [iat: (System.currentTimeMillis().intdiv(1000))]

    def enc    = { obj ->
        Base64.urlEncoder
              .withoutPadding()
              .encodeToString(obj as String == obj ? obj : new groovy.json.JsonBuilder(obj).toString()
                                .getBytes(StandardCharsets.UTF_8))
    }

    return "${enc(new groovy.json.JsonBuilder(header))}.${enc(new groovy.json.JsonBuilder(claim))}"
}

/* -----------------------  buildSignature  -------------------------------------- */
/* Signiert den JWT-Token mittels HmacSHA256                                       */
private String buildSignature(String jwtToken, String hmacKey) {

    Mac mac = Mac.getInstance('HmacSHA256')
    mac.init(new SecretKeySpec(hmacKey.getBytes(StandardCharsets.UTF_8), 'HmacSHA256'))
    byte[] signatureBytes = mac.doFinal(jwtToken.getBytes(StandardCharsets.UTF_8))
    return Base64.encoder.encodeToString(signatureBytes)
}

/* -----------------------  callGetAccount  -------------------------------------- */
/* Ruft das CDC-Account-API auf, liefert das Profil-Objekt zurück                   */
private Map callGetAccount(String baseUrl,
                           String accountType,
                           String profileId,
                           String user,
                           String password,
                           String jwtToken,
                           String cdcSignature,
                           def   msgLog) {

    def url = new URL("${baseUrl}/${accountType}/${profileId}")
    def con = (HttpURLConnection) url.openConnection()
    con.with {
        requestMethod  = 'GET'
        connectTimeout = 10000
        readTimeout    = 10000
        setRequestProperty('Authorization',
                'Basic ' + "${user}:${password}".bytes.encodeBase64().toString())
        setRequestProperty('JWT-Token',   jwtToken)
        setRequestProperty('CDCSignature', cdcSignature)
    }

    int rc = con.responseCode
    if (rc != 200) {
        throw new RuntimeException("CDC-GET Account HTTP RC=${rc}")
    }

    def responseText = con.inputStream.getText('UTF-8')
    msgLog?.addAttachmentAsString('CDC_GetAccount_Response', responseText, 'application/json')

    def jsonResp = new JsonSlurper().parseText(responseText)
    if (!jsonResp?.profile) {
        throw new RuntimeException('Profil-Objekt in CDC-Antwort fehlt')
    }
    return jsonResp.profile as Map
}

/* -----------------------  validateProfile  ------------------------------------- */
/* Prüft, ob Pflichtfelder innerhalb des Profils vorhanden sind                    */
private void validateProfile(Map profile, def msgLog) {

    if (!profile) {
        throw new IllegalArgumentException('Profil ist leer oder null')
    }
    if (!profile.lastName) {
        throw new IllegalArgumentException('Pflichtfeld lastName fehlt im Profil')
    }
    /* Optionale Info ins Log */
    msgLog?.addAttachmentAsString('Validated_Profile',
            new groovy.json.JsonBuilder(profile).toPrettyString(), 'application/json')
}

/* -----------------------  mapProfileToMdmXml  ---------------------------------- */
/* Führt das JSON-> XML Mapping gemäß Spezifikation aus                            */
private String mapProfileToMdmXml(Map profileObj) {

    def uuid   = UUID.randomUUID().toString()
    def emails = profileObj.emails?.verified ?: []

    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)

    xml.'bp:BusinessPartnerSUITEBulkReplicateRequest'(
            'xmlns:bp':'urn:sap-com:document:sap:rfc:functions') {
        BusinessPartnerSUITEReplicateRequestMessage {
            MessageHeader {
                UUID uuid
                ID  uuid.replaceAll('-','')
            }
            BusinessPartner {
                Common {
                    KeyWordsText           profileObj.firstName ?: ''
                    AdditionalKeyWordsText profileObj.lastName  ?: ''
                    Person {
                        Name {
                            GivenName  profileObj.firstName ?: ''
                            FamilyName profileObj.lastName  ?: ''
                        }
                    }
                }
                AddressIndependentCommInfo {
                    emails.each { e ->
                        Email {
                            URI e.email
                        }
                    }
                }
            }
        }
    }
    return sw.toString()
}

/* -----------------------  handleError  ----------------------------------------- */
/* Zentrales Error-Handling                                                       */
private void handleError(String originalBody, Exception e, def msgLog) {

    msgLog?.addAttachmentAsString('ErrorPayload', originalBody ?: '', 'text/plain')
    def errMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}