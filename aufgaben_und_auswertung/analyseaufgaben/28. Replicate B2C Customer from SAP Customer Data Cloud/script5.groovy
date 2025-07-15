/*****************************************************************************************
 *  Groovy-Script  –  SAP Cloud Integration
 *  -------------------------------------------------------------------------
 *  Aufgabe:
 *    - Verarbeitung von CDC-Events und Replikation der zugehörigen Profile in das
 *      SAP Master Data Management – gemäß der in der Aufgabenstellung
 *      beschriebenen Anforderungen.
 *
 *  Autor:  ChatGPT (Senior-Integration-Dev)
 *****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.util.Base64
import java.nio.charset.StandardCharsets
import java.util.UUID

/*****************************************************************************************
 *  Haupt-Einstiegspunkt
 *****************************************************************************************/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    def originalBody = message.getBody(String) ?: ''

    try {

        /* =====================================================================
         *  1. Header / Property Handling
         * =================================================================== */
        def ctx = initContextValues(message, messageLog)

        /* =====================================================================
         *  2. Event-Validierung
         * =================================================================== */
        def event = extractSingleEvent(originalBody)
        validateEvent(event, messageLog)

        /* =====================================================================
         *  3. Profil-UID und AccountType in Properties ablegen
         * =================================================================== */
        String profileId  = event.data.uid
        String accountTyp = event.data.accountType
        message.setProperty('profileID' , profileId )
        message.setProperty('accountType', accountTyp)

        /* =====================================================================
         *  4. JWT-Token bilden + signieren
         * =================================================================== */
        String jwtToken  = buildJwtToken(ctx.apiKey)
        String signature = sign(jwtToken, ctx.plainHMACKeyForSignature)

        /* =====================================================================
         *  5. GET Account – CDC
         * =================================================================== */
        def profileJson = callGetAccount(ctx, accountTyp, profileId, jwtToken, signature, messageLog)

        /* =====================================================================
         *  6. Profil-Validierung
         * =================================================================== */
        validateProfile(profileJson, messageLog)

        /* =====================================================================
         *  7. Mapping JSON → XML
         * =================================================================== */
        String mdmPayload = mapProfileToMDM(profileJson)

        /* =====================================================================
         *  8. POST Replikation – MDM
         * =================================================================== */
        callReplicateToMDM(ctx, profileId, mdmPayload, messageLog)

        /* =====================================================================
         *  9. XML im Message-Body ablegen
         * =================================================================== */
        message.setBody(mdmPayload)
        message.setHeader('Content-Type', 'application/xml')

        return message

    } catch (Exception ex) {
        handleError(originalBody, ex, messageLog)
        return message   // wird nie erreicht, handleError wirft Exception
    }
}

/*****************************************************************************************
 *  Funktion: initContextValues
 *  -------------------------------------------------------------------------
 *  Füllt ein Map-Objekt mit allen benötigten Properties/Headers. Fehlt ein
 *  Eintrag, wird er mit „placeholder“ ersetzt.
 *****************************************************************************************/
private Map initContextValues(Message msg, def log) {

    def propNames = [
        'requestUser', 'requestPassword', 'requestURL',
        'requestURLMDM', 'requestUserMDM', 'requestPasswordMDM',
        'plainHMACKeyForSignature', 'apiKey'
    ]

    def ctx = [:]
    propNames.each { p ->
        def val = msg.getProperty(p) ?: 'placeholder'
        ctx[p] = val
        msg.setProperty(p, val)           // stellt sicher, dass Property vorhanden ist
    }

    // gleiches Prozedere für evtl. Header (hier exemplarisch leer)
    return ctx
}

/*****************************************************************************************
 *  Funktion: extractSingleEvent
 *  -------------------------------------------------------------------------
 *  Parsed den Request-Body (JSON) und liefert das erste Event-Objekt zurück.
 *****************************************************************************************/
private Map extractSingleEvent(String bodyText) {
    def json = new JsonSlurper().parseText(bodyText ?: '{}') as Map
    if (!(json?.events instanceof List) || json.events.isEmpty())
        throw new IllegalArgumentException('Kein Events-Array im Request gefunden.')
    return json.events[0] as Map
}

/*****************************************************************************************
 *  Funktion: validateEvent
 *  -------------------------------------------------------------------------
 *  Prüft das Event anhand der Vorgaben.
 *****************************************************************************************/
private void validateEvent(Map event, def log) {

    List<String> allowedTypes = ['accountCreated', 'accountUpdated', 'accountRegistered']
    if (!allowedTypes.contains(event.type))
        throw new IllegalArgumentException("Ungültiger Event-Typ: ${event.type}")

    if (!event?.data?.uid)
        throw new IllegalArgumentException('UID fehlt im Event.')

    log?.addAttachmentAsString('EventValid', JsonOutput.prettyPrint(JsonOutput.toJson(event)), 'application/json')
}

/*****************************************************************************************
 *  Funktion: buildJwtToken
 *  -------------------------------------------------------------------------
 *  Erstellt den JWT-Token gem. Spezifikation (Header + Claim, Base64-URL-Kodiert)
 *****************************************************************************************/
private String buildJwtToken(String apiKey) {

    def header  = [alg: 'RS256', typ: 'JWT', kid: apiKey]
    def claim   = [iat: (System.currentTimeMillis() / 1000L) as long]

    def hdrEnc  = base64UrlEncode(JsonOutput.toJson(header))
    def clmEnc  = base64UrlEncode(JsonOutput.toJson(claim))

    return "${hdrEnc}.${clmEnc}"
}

/*****************************************************************************************
 *  Funktion: sign
 *  -------------------------------------------------------------------------
 *  Signiert den JWT-Token via HMAC-SHA256 und gibt Base64URL-Ergebnis zurück.
 *****************************************************************************************/
private String sign(String token, String plainKey) {

    Mac mac = Mac.getInstance('HmacSHA256')
    mac.init(new SecretKeySpec(plainKey.getBytes(StandardCharsets.UTF_8), 'HmacSHA256'))
    byte[] rawHmac = mac.doFinal(token.getBytes(StandardCharsets.UTF_8))
    return base64UrlEncode(rawHmac)
}

/*****************************************************************************************
 *  Funktion: callGetAccount
 *  -------------------------------------------------------------------------
 *  Führt den GET-Aufruf gegen CDC aus und liefert das „profile“-Objekt zurück.
 *****************************************************************************************/
private Map callGetAccount(Map      ctx,
                           String   accountType,
                           String   profileId,
                           String   jwtToken,
                           String   signature,
                           def      log) {

    def urlStr = "${ctx.requestURL}/${accountType}/${profileId}"
    def conn   = new URL(urlStr).openConnection()
    conn.setRequestMethod('GET')

    // Basic-Auth Header
    String basicAuth = "${ctx.requestUser}:${ctx.requestPassword}"
    conn.setRequestProperty('Authorization', 'Basic ' + Base64.encoder.encodeToString(basicAuth.bytes))

    // CDC Header
    conn.setRequestProperty('JWT-Token'   , jwtToken)
    conn.setRequestProperty('CDCSignature', signature)

    int rc = conn.responseCode
    if (rc != 200) {
        throw new RuntimeException("GET Account returned HTTP ${rc}")
    }

    def respBody = conn.inputStream.getText('UTF-8')
    log?.addAttachmentAsString('CDC-Response', respBody, 'application/json')

    def parsed   = new JsonSlurper().parseText(respBody) as Map
    return (parsed.profile ?: [:]) as Map
}

/*****************************************************************************************
 *  Funktion: validateProfile
 *  -------------------------------------------------------------------------
 *  Validiert das empfangene Profil gem. Vorgaben.
 *****************************************************************************************/
private void validateProfile(Map profile, def log) {
    if (!profile)
        throw new IllegalArgumentException('Profil fehlt in der Antwort.')

    if (!profile.lastName)
        throw new IllegalArgumentException('Pflichtfeld lastName fehlt im Profil.')

    log?.addAttachmentAsString('ProfileValid', JsonOutput.prettyPrint(JsonOutput.toJson(profile)), 'application/json')
}

/*****************************************************************************************
 *  Funktion: mapProfileToMDM
 *  -------------------------------------------------------------------------
 *  Erstellt das benötigte XML-Payload für MDM inklusive Namespace.
 *****************************************************************************************/
private String mapProfileToMDM(Map profile) {

    String uuidStr = UUID.randomUUID().toString()
    String idStr   = uuidStr.replaceAll('-', '')

    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)
    xml.'bp:BusinessPartnerSUITEBulkReplicateRequest'('xmlns:bp': 'urn:sap-com:document:sap:rfc:functions') {
        BusinessPartnerSUITEReplicateRequestMessage {
            MessageHeader {
                UUID(uuidStr)
                ID(idStr)
            }
            BusinessPartner {
                Common {
                    KeyWordsText(profile.firstName ?: '')
                    AdditionalKeyWordsText(profile.lastName ?: '')
                    Person {
                        Name {
                            GivenName(profile.firstName ?: '')
                            FamilyName(profile.lastName ?: '')
                        }
                    }
                }
                AddressIndependentCommInfo {
                    buildEmailNodes(xml, profile)
                }
            }
        }
    }
    return sw.toString()
}

/*****************************************************************************************
 *  Hilfs-Funktion: buildEmailNodes
 *  -------------------------------------------------------------------------
 *  Fügt beliebig viele <Email>-Knoten hinzu (verified-Liste).
 *****************************************************************************************/
private void buildEmailNodes(def xmlBuilder, Map profile) {

    def verifiedList = (profile.emails?.verified ?: []) as List
    if (verifiedList.isEmpty() && profile.email)
        verifiedList = [[email: profile.email]]

    verifiedList.each { mail ->
        if (!mail?.email) return
        xmlBuilder.Email {
            URI(mail.email)
        }
    }
}

/*****************************************************************************************
 *  Funktion: callReplicateToMDM
 *  -------------------------------------------------------------------------
 *  POST der erzeugten XML an MDM-Endpunkt.
 *****************************************************************************************/
private void callReplicateToMDM(Map ctx, String profileId, String payload, def log) {

    String urlStr = "${ctx.requestURLMDM}/${profileId}"
    def conn = new URL(urlStr).openConnection()
    conn.setRequestMethod('POST')

    String basicAuth = "${ctx.requestUserMDM}:${ctx.requestPasswordMDM}"
    conn.setRequestProperty('Authorization', 'Basic ' + Base64.encoder.encodeToString(basicAuth.bytes))
    conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
    conn.doOutput = true
    conn.outputStream.withWriter('UTF-8') { it << payload }

    int rc = conn.responseCode
    if (rc >= 300) {
        String err = conn.errorStream?.getText('UTF-8') ?: ''
        log?.addAttachmentAsString('MDM-Error', err, 'text/plain')
        throw new RuntimeException("Replikation an MDM fehlgeschlagen (HTTP ${rc})")
    }

    log?.setStringProperty('MDM-Replication', "Erfolgreich (HTTP ${rc})")
}

/*****************************************************************************************
 *  Funktion: base64UrlEncode
 *  -------------------------------------------------------------------------
 *  Standard-Base64 → URL-/Filename-sicher ohne Padding.
 *****************************************************************************************/
private static String base64UrlEncode(def data) {
    byte[] bytes = (data instanceof byte[]) ? data : data.toString().bytes
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
}

/*****************************************************************************************
 *  Funktion: handleError
 *  -------------------------------------------------------------------------
 *  Einheitliches Fehler-Handling. Fügt Payload als Attachment hinzu
 *  und wirft RuntimeException weiter.
 *****************************************************************************************/
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/plain')
    def errorMsg = "Fehler im Integration-Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}