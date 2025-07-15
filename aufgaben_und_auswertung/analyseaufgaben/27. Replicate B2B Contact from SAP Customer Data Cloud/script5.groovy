/****************************************************************************************************************
 *  Groovy-Skript – CDC → SAP MDM (B2B) ­Integration
 *
 *  Autor:      Senior Integration Developer
 *  Version:    1.0
 *  Datum:      2025-06-18
 *
 *  Beschreibung:
 *  Dieses Skript liest CDC-Events, ruft über die CDC REST-API das vollständige Profil ab, prüft es auf
 *  definierte Validierungsregeln, mappt die Daten in das MDM-XML-Format und ruft anschließend die
 *  MDM-API auf, um den Business Partner zu replizieren.
 *
 *  Modularität:
 *  ┌──────────────────────────────────────────────────────────────────────────────┐
 *  │ 1)  setContextData()            – Liest/initialisiert Header & Properties   │
 *  │ 2)  createJwtToken()            – Erstellt JWT ohne Signatur                │
 *  │ 3)  callCdcGetAccount()         – GET  Profil aus CDC                       │
 *  │ 4)  validateProfile()           – Führt Business-Validierung durch          │
 *  │ 5)  mapProfileToMdmXml()        – Baut Zieldokument auf                     │
 *  │ 6)  callMdmReplication()        – POST zum MDM                              │
 *  │ 7)  handleError()               – Zentrales Error-Handling                  │
 *  └──────────────────────────────────────────────────────────────────────────────┘
 ****************************************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.xml.MarkupBuilder
import java.time.Instant
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    def ctx = setContextData(message)                               // 1) Properties & Header

    try {
        /*======================================================================
          Event-Objekt ermitteln (nach Splitter besteht der Body i.d.R. nur aus
          dem einzelnen Event; andernfalls wird das erste Element verwendet).   */
        def rawBody        = message.getBody(String)
        def jsonSlurper    = new JsonSlurper()
        def parsedJson     = jsonSlurper.parseText(rawBody ?: '{}')
        def eventObj       = parsedJson?.events ? parsedJson.events[0] : parsedJson
        def profileUid     = eventObj?.data?.uid ?: ''

        if (!profileUid) {
            throw new Exception("Kein Feld 'uid' im Event gefunden.")
        }
        message.setProperty('profileID', profileUid)

        /*======================================================================
          3) JWT & Signature erzeugen                                           */
        def jwtToken       = createJwtToken(ctx.apiKey)
        def cdcSignature   = sign(jwtToken, ctx.plainHMACKeyForSignature)

        /*======================================================================
          5) GET Account aus CDC holen                                          */
        def fullProfileJson = callCdcGetAccount(profileUid, jwtToken, cdcSignature, ctx, messageLog)

        /*======================================================================
          6) Business-Validierung                                               */
        validateProfile(fullProfileJson, messageLog)

        /*======================================================================
          4 & 7) Mapping und POST ans MDM                                       */
        def mdmXml = mapProfileToMdmXml(fullProfileJson, messageLog)
        callMdmReplication(profileUid, mdmXml, ctx, messageLog)

        /*======================================================================
          Ergebnis in Message-Body schreiben                                    */
        message.setBody(mdmXml)
        return message

    } catch (Exception ex) {
        handleError(message.getBody(String), ex, messageLog)
        return message   // wird nie erreicht; handleError wirft Exception
    }
}

/*───────────────────────────────────────────────────────────────────────────────
 *  Function: setContextData
 *  Liest alle benötigten Properties & Header. Fehlen Werte, werden sie mit
 *  'placeholder' initialisiert.
 *──────────────────────────────────────────────────────────────────────────────*/
def setContextData(Message message) {
    Map ctx = [:]

    ['requestUser',
     'requestPassword',
     'requestURL',
     'requestURLMDM',
     'requestUserMDM',
     'requestPasswordMDM',
     'plainHMACKeyForSignature',
     'apiKey'].each { key ->
        def val = message.getProperty(key) ?: 'placeholder'
        ctx[key] = val
    }

    // Fallback auf Default-Werte laut Aufgabenstellung
    if (ctx.plainHMACKeyForSignature == 'placeholder') ctx.plainHMACKeyForSignature = 'gekmN8CFr6PngJ9xTGMOMpdTFAvcssg5'
    if (ctx.apiKey                   == 'placeholder') ctx.apiKey                   = 'API_KEY_PROD1'

    return ctx
}

/*───────────────────────────────────────────────────────────────────────────────
 *  Function: createJwtToken
 *  Erstellt Base64Url-kodierten JWT-Header & Claim (ohne Signatur) und
 *  liefert den zusammengesetzten Token {header}.{claim} zurück.
 *──────────────────────────────────────────────────────────────────────────────*/
def createJwtToken(String apiKey) {
    def headerJson = [alg: 'RS256', typ: 'JWT', kid: apiKey]
    def claimJson  = [ts : Instant.now().epochSecond]               // einfacher Zeitstempel-Claim

    def enc = { obj ->
        Base64.getUrlEncoder().withoutPadding()
              .encodeToString(obj.toString().getBytes('UTF-8'))
    }
    return "${enc(headerJson)}.${enc(claimJson)}"
}

/*───────────────────────────────────────────────────────────────────────────────
 *  Function: sign
 *  Signiert den JWT-Token mittels HMAC-SHA256 & liefert Base64Url-Signature.
 *──────────────────────────────────────────────────────────────────────────────*/
def sign(String token, String key) {
    Mac mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key.getBytes('UTF-8'), "HmacSHA256"))
    byte[] rawHmac = mac.doFinal(token.getBytes('UTF-8'))
    return Base64.getUrlEncoder().withoutPadding().encodeToString(rawHmac)
}

/*───────────────────────────────────────────────────────────────────────────────
 *  Function: callCdcGetAccount
 *  Ruft den CDC Endpunkt GET {requestURL}/{uid} auf und gibt das geparste
 *  JSON-Objekt zurück.
 *──────────────────────────────────────────────────────────────────────────────*/
def callCdcGetAccount(String uid,
                      String jwt,
                      String signature,
                      Map    ctx,
                      def    log) {

    def url = new URL("${ctx.requestURL}/${uid}")
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('GET')
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(15000)

    def authBytes = "${ctx.requestUser}:${ctx.requestPassword}".getBytes('UTF-8')
    conn.setRequestProperty('Authorization', 'Basic ' +
            Base64.encoder.encodeToString(authBytes))
    conn.setRequestProperty('JWT-Token',    jwt)
    conn.setRequestProperty('CDCSignature', signature)

    int rc = conn.responseCode
    log?.addAttachmentAsString("CDC-GET-RC", rc as String, 'text/plain')

    if (rc != 200) {
        throw new Exception("CDC GET-Account Response Code: ${rc}")
    }
    String responseJson = conn.inputStream.getText('UTF-8')
    return new JsonSlurper().parseText(responseJson)
}

/*───────────────────────────────────────────────────────────────────────────────
 *  Function: validateProfile
 *  Prüft isRegistered = true, Existenz von profile & profile.lastName.
 *──────────────────────────────────────────────────────────────────────────────*/
def validateProfile(def profileJson, def log) {
    if (!profileJson?.isRegistered) {
        throw new Exception("Validierung fehlgeschlagen: isRegistered != true.")
    }
    if (!profileJson?.profile) {
        throw new Exception("Validierung fehlgeschlagen: Profil-Objekt fehlt.")
    }
    if (!profileJson.profile?.lastName) {
        throw new Exception("Validierung fehlgeschlagen: lastName fehlt.")
    }
    log?.addAttachmentAsString("Validation", "Profil erfolgreich validiert", "text/plain")
}

/*───────────────────────────────────────────────────────────────────────────────
 *  Function: mapProfileToMdmXml
 *  Wandelt das JSON-Profil in das geforderte XML-Format um.
 *──────────────────────────────────────────────────────────────────────────────*/
def mapProfileToMdmXml(def profileJson, def log) {

    String uuid          = UUID.randomUUID().toString()
    String idNoHyphens   = uuid.replaceAll('-', '')

    StringWriter sw      = new StringWriter()
    def xml              = new MarkupBuilder(sw)

    xml.'bp:BusinessPartnerSUITEBulkReplicateRequest'('xmlns:bp': 'urn:sap-com:document:sap:rfc:functions') {
        BusinessPartnerSUITEReplicateRequestMessage {
            MessageHeader {
                UUID(uuid)
                ID(idNoHyphens)
            }
            BusinessPartner {
                Common {
                    KeyWordsText(profileJson.profile.firstName ?: '')
                    AdditionalKeyWordsText(profileJson.profile.lastName ?: '')
                    Person {
                        Name {
                            GivenName(profileJson.profile.firstName ?: '')
                            FamilyName(profileJson.profile.lastName ?: '')
                        }
                    }
                }
                AddressIndependentCommInfo {
                    /*-----------------------------------------------------------
                      Für jede verifizierte E-Mail einen <Email><URI/></Email>
                     -----------------------------------------------------------*/
                    def verifiedEmails = profileJson?.emails?.verified ?: []
                    verifiedEmails.each { mailObj ->
                        if (mailObj?.email) {
                            Email {
                                URI(mailObj.email)
                            }
                        }
                    }
                }
            }
        }
    }
    def xmlString = sw.toString()
    log?.addAttachmentAsString("MDM-XML", xmlString, "application/xml")
    return xmlString
}

/*───────────────────────────────────────────────────────────────────────────────
 *  Function: callMdmReplication
 *  POST Aufruf an das MDM-Endpunkt mit Basic-Auth.
 *──────────────────────────────────────────────────────────────────────────────*/
def callMdmReplication(String uid, String xmlBody, Map ctx, def log) {

    def url = new URL("${ctx.requestURLMDM}/${uid}")
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Content-Type', 'application/xml;charset=UTF-8')

    def authBytes = "${ctx.requestUserMDM}:${ctx.requestPasswordMDM}".getBytes('UTF-8')
    conn.setRequestProperty('Authorization', 'Basic ' +
            Base64.encoder.encodeToString(authBytes))

    conn.outputStream.withWriter('UTF-8') { it << xmlBody }
    int rc = conn.responseCode
    log?.addAttachmentAsString("MDM-POST-RC", rc as String, 'text/plain')

    if (rc >= 300) {
        throw new Exception("MDM Replication fehlgeschlagen. HTTP RC: ${rc}")
    }
}

/*───────────────────────────────────────────────────────────────────────────────
 *  Function: handleError
 *  Zentrales Error-Handling gemäß Vorgabe.
 *──────────────────────────────────────────────────────────────────────────────*/
def handleError(String body, Exception e, def messageLog) {
    // Logging im Monitoring (Name, Inhalt, Typ)
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: '', "text/xml")
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}