/*****************************************************************************************
*  SAP Cloud Integration – Groovy-Skript
*  Aufgabe: CDC-B2B-Profil­daten an SAP MDI weiterleiten
*
*  Autor:   (Senior-Integration-Developer)
*  Version: 1.0
*
*  Hinweise:
*  – Das Skript wird OHNE weitere Abhängigkeiten ausgeführt.
*  – Alle Funktionen sind modular aufgebaut und enthalten deutsches Logging.
*  – Fehler verursachen RuntimeExceptions, der ursprüngliche Payload wird als
*    Attachment mitgeführt (siehe handleError).
*****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import java.time.Instant
import java.nio.charset.StandardCharsets

/*****************************************************************************************
*  Einstiegspunkt
*****************************************************************************************/
Message processData(Message message) {

    // Zugriff auf das MessageLog
    def messageLog = messageLogFactory.getMessageLog(message)
    def rawBody    = message.getBody(String) as String

    try {
        /* ----------------------------------------------------------------------
         * 1. Kontext vorbereiten (Header & Properties)  
         * -------------------------------------------------------------------- */
        Map<String, String> ctx = prepareContext(message)

        /* ----------------------------------------------------------------------
         * 2. Eingehenden JSON-Payload parsen
         * -------------------------------------------------------------------- */
        def json = new JsonSlurper().parseText(rawBody)

        if (!(json?.events instanceof List) || json.events.isEmpty()) {
            throw new IllegalArgumentException("Eingangspayload enthält kein gültiges 'events'-Array.")
        }

        /* ----------------------------------------------------------------------
         * 3. Jedes Event einzeln verarbeiten
         * -------------------------------------------------------------------- */
        json.events.each { evt ->
            try {
                processSingleEvent(evt, ctx, message, messageLog)
            } catch (Exception exEvt) {
                handleError(JsonOutput.prettyPrint(JsonOutput.toJson(evt)), exEvt, messageLog)
            }
        }

        return message
    } catch (Exception e) {
        /* Gesamtabbruch */
        handleError(rawBody, e, messageLog)   // wirft RuntimeException weiter
    }
}

/*****************************************************************************************
*  Verarbeitet ein einzelnes Event-Objekt (Splitter-Logik)
*****************************************************************************************/
void processSingleEvent(def event, Map ctx, Message message, def messageLog) {

    /* 3.1 JWT-Token + Signatur erzeugen */
    String jwt       = createJwtToken(ctx.apiKey)
    String signature = signJwt(jwt, ctx.plainHMACKeyForSignature)

    /* 3.2 UID aus Event lesen und als Property setzen */
    String profileId = event?.data?.uid ?: 'unknown'
    message.setProperty("profileID", profileId)

    /* 3.3 Get-Account-Aufruf */
    String cdcUrl  = "${ctx.requestURL}/${profileId}"
    String accountJsonStr = callGetAccount(cdcUrl, ctx.requestUser, ctx.requestPassword,
                                           jwt, signature, messageLog)

    /* 3.4 Validierung des zurückgelieferten Profils */
    def accountJson = new JsonSlurper().parseText(accountJsonStr)
    validateAccount(accountJson)

    /* 3.5 Mapping in Ziel-XML */
    String mdmXml = mapToMdmXml(accountJson)

    /* 3.6 Aufruf MDM-API */
    String mdmUrl = "${ctx.requestURLMDM}/${profileId}"
    callReplicateProfile(mdmUrl, ctx.requestUserMDM, ctx.requestPasswordMDM,
                         mdmXml, messageLog)

    /* 3.7 Optional – das zuletzt erzeugte XML im Message-Body ablegen */
    message.setBody(mdmXml)
}

/*****************************************************************************************
*  Liest alle benötigten Header & Properties – ersetzt fehlende Werte durch 'placeholder'
*****************************************************************************************/
Map<String, String> prepareContext(Message msg) {
    Map ctx = [:]
    [
        'requestUser', 'requestPassword', 'requestURL',
        'requestURLMDM', 'requestUserMDM', 'requestPasswordMDM',
        'plainHMACKeyForSignature', 'apiKey'
    ].each { key ->
        ctx[key] = (msg.getProperty(key) ?: msg.getHeader(key, String) ?: 'placeholder')
    }
    return ctx
}

/*****************************************************************************************
*  JWT-Token (Header.Claim) erstellen
*****************************************************************************************/
String createJwtToken(String apiKey) {
    String headerB64 = JsonOutput.toJson([alg:'RS256', typ:'JWT', kid:apiKey])
                          .bytes.encodeBase64().toString()
    String claimB64  = JsonOutput.toJson([iat: Instant.now().epochSecond])
                          .bytes.encodeBase64().toString()
    return "${headerB64}.${claimB64}"
}

/*****************************************************************************************
*  JWT‐Token mittels HMAC-SHA256 signieren
*****************************************************************************************/
String signJwt(String jwt, String plainKey) {
    Mac mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(plainKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
    return mac.doFinal(jwt.getBytes(StandardCharsets.UTF_8)).encodeBase64().toString()
}

/*****************************************************************************************
*  API-Call: Get Account (CDC)
*****************************************************************************************/
String callGetAccount(String urlString, String user, String pwd,
                      String jwt, String signature, def messageLog) {
    try {
        def conn = new URL(urlString).openConnection() as HttpURLConnection
        conn.requestMethod = "GET"
        conn.setRequestProperty("Authorization", "Basic ${"${user}:${pwd}".bytes.encodeBase64()}")
        conn.setRequestProperty("JWT-Token",   jwt)
        conn.setRequestProperty("CDCSignature", signature)

        int rc = conn.responseCode
        if (rc != 200) {
            throw new RuntimeException("CDC-GetAccount antwortet mit HTTP ${rc}: ${conn.responseMessage}")
        }
        String resp = conn.inputStream.getText("UTF-8")
        messageLog?.addAttachmentAsString("GetAccountResponse_${rc}", resp, "application/json")
        return resp
    } catch (Exception e) {
        handleError("{}", e, messageLog)        // wirft Exception weiter
    }
}

/*****************************************************************************************
*  Validierung gemäß Vorgabe
*****************************************************************************************/
void validateAccount(def acc) {
    if (!acc?.isRegistered)               throw new IllegalArgumentException("'isRegistered' != true")
    if (!acc?.profile)                    throw new IllegalArgumentException("'profile' fehlt")
    if (!acc?.profile?.lastName)          throw new IllegalArgumentException("'profile.lastName' fehlt")
}

/*****************************************************************************************
*  Mapping JSON → XML (MDM-Format)
*****************************************************************************************/
String mapToMdmXml(def acc) {

    String uuid         = UUID.randomUUID().toString()
    String idWithoutDash = uuid.replaceAll('-', '')

    def sw  = new StringWriter()
    def xml = new MarkupBuilder(sw)

    xml.'bp:BusinessPartnerSUITEBulkReplicateRequest'(
        'xmlns:bp':'urn:sap-com:document:sap:rfc:functions') {
        BusinessPartnerSUITEReplicateRequestMessage {
            MessageHeader {
                UUID(uuid)
                ID(idWithoutDash)
            }
            BusinessPartner {
                Common {
                    KeyWordsText(acc.profile.firstName)
                    AdditionalKeyWordsText(acc.profile.lastName)
                    Person {
                        Name {
                            GivenName(acc.profile.firstName)
                            FamilyName(acc.profile.lastName)
                        }
                    }
                }
                AddressIndependentCommInfo {
                    acc.emails?.verified?.each { mail ->
                        if (mail?.email) {
                            Email {
                                URI(mail.email)
                            }
                        }
                    }
                }
            }
        }
    }
    return sw.toString()
}

/*****************************************************************************************
*  API-Call: Replicate Profile to MDM
*****************************************************************************************/
void callReplicateProfile(String urlString, String user, String pwd,
                          String xmlPayload, def messageLog) {

    try {
        def conn = new URL(urlString).openConnection() as HttpURLConnection
        conn.requestMethod = "POST"
        conn.doOutput      = true
        conn.setRequestProperty("Content-Type", "application/xml; charset=UTF-8")
        conn.setRequestProperty("Authorization", "Basic ${"${user}:${pwd}".bytes.encodeBase64()}")

        conn.outputStream.withWriter("UTF-8") { it << xmlPayload }

        int rc = conn.responseCode
        if (rc < 200 || rc >= 300) {
            String err = conn.errorStream?.getText("UTF-8") ?: ""
            throw new RuntimeException("MDM-Replikation fehlgeschlagen – HTTP ${rc}: ${err}")
        }
        messageLog?.addAttachmentAsString("ReplicateProfileResponse", "HTTP ${rc}", "text/plain")
    } catch (Exception e) {
        handleError(xmlPayload, e, messageLog)
    }
}

/*****************************************************************************************
*  Zentrales Error-Handling (Attachment & Exception)
*****************************************************************************************/
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "", "text/plain")
    throw new RuntimeException("Fehler im Mapping-Skript: ${e.message}", e)
}