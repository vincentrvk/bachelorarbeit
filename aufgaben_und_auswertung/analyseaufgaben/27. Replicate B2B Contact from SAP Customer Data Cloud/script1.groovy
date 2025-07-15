/*****************************************************************************
*  SAP Cloud Integration – Groovy-Skript                                      *
*  Beschreibung:                                                             *
*  – Holt ein Benutzer-Profil aus der SAP Customer Data Cloud                *
*  – Validiert das Profil gem. Vorgabe                                       *
*  – Erstellt ein XML gem. MDM-Zielschema                                    *
*  – Repliziert das Profil nach SAP MDM                                       *
*                                                                             *
*  Autor:   AI-Assistent                                                     *
*  Version: 1.0                                                              *
*****************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.net.HttpURLConnection
import java.util.Base64
import java.util.UUID
import java.nio.charset.StandardCharsets

/*******************************  MAIN  *************************************/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        /* 1. Header / Property-Initialisierung */
        def config = prepareConfig(message, messageLog)

        /* 2. Eingehender Body (events-Array) parsen                           */
        def body            = message.getBody(String) ?: '{}'
        def jsonIn          = new JsonSlurper().parseText(body)
        def events          = jsonIn?.events ?: []

        /* 3. Jeden Event einzeln verarbeiten                                  */
        events.each { eventObj ->

            def profileId = eventObj?.data?.uid
            if(!profileId){
                throw new RuntimeException("UID im Event nicht vorhanden.")
            }

            /* Property setzen, damit nachfolgende IFlow-Schritte sie lesen können */
            message.setProperty("profileID", profileId)

            /* 4. JWT & Signatur erzeugen                                       */
            def jwt       = createJWT(config.apiKey)
            def signature = signJWT(jwt, config.hmacKey)

            /* 5. CDC-API: Account abrufen                                      */
            def cdcResponse = getAccount(config, profileId, jwt, signature, messageLog)

            /* 6. Validierung Account-Response                                  */
            validateProfile(cdcResponse)

            /* 7. Mapping nach MDM-XML                                          */
            def mdmXml = mapToMDM(cdcResponse, messageLog)

            /* 8. Replikation nach SAP MDM                                      */
            replicateToMDM(config, profileId, mdmXml, messageLog)
        }

        /* Letztes erstelltes XML als Body zurückgeben                          */
        message.setBody("Prozessierung abgeschlossen.")
        return message

    } catch(Exception e){
        handleError(message.getBody(String), e, messageLog)
    }
}

/******************************* FUNCTIONS **********************************/

/*-------------------------------------------------------------------------*/
/*  prepareConfig – Liest Properties / Header und liefert Konfig-Map       */
/*-------------------------------------------------------------------------*/
Map prepareConfig(Message msg, def log){
    def readVal = { String key ->
        def v = msg.getProperty(key) ?: msg.getHeader(key, String)
        return (v ? v.toString() : 'placeholder')
    }

    Map cfg = [
            requestUser      : readVal('requestUser'),
            requestPassword  : readVal('requestPassword'),
            requestURL       : readVal('requestURL'),
            requestURLMDM    : readVal('requestURLMDM'),
            requestUserMDM   : readVal('requestUserMDM'),
            requestPasswordMDM: readVal('requestPasswordMDM'),
            hmacKey          : readVal('plainHMACKeyForSignature'),
            apiKey           : readVal('apiKey')
    ]
    log?.addAttachmentAsString('Konfiguration', JsonOutput.prettyPrint(JsonOutput.toJson(cfg)), 'application/json')
    return cfg
}

/*-------------------------------------------------------------------------*/
/*  createJWT – Erstellt einfaches JWT (Header + Claim)                    */
/*-------------------------------------------------------------------------*/
String createJWT(String apiKey){
    def headerJson   = JsonOutput.toJson([alg:'RS256',typ:'JWT',kid: apiKey])
    def claimJson    = JsonOutput.toJson([iat: (System.currentTimeMillis()/1000L) as long])

    def enc = { String s ->
        Base64.encoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))
                .replace('+','-').replace('/','_').replace('=','')
    }

    return "${enc(headerJson)}.${enc(claimJson)}"
}

/*-------------------------------------------------------------------------*/
/*  signJWT – Signiert Token mit HMAC-SHA256                               */
/*-------------------------------------------------------------------------*/
String signJWT(String token, String key){
    Mac mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
    byte[] raw = mac.doFinal(token.getBytes(StandardCharsets.UTF_8))
    return Base64.encoder.encodeToString(raw).replace('+','-').replace('/','_').replace('=','')
}

/*-------------------------------------------------------------------------*/
/*  getAccount – Führt HTTP-GET gegen CDC aus                              */
/*-------------------------------------------------------------------------*/
Map getAccount(Map cfg, String profileId, String jwt, String signature, def log){
    String urlStr = "${cfg.requestURL}/${profileId}"
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod("GET")

    String basic = "${cfg.requestUser}:${cfg.requestPassword}".bytes.encodeBase64().toString()
    conn.setRequestProperty("Authorization", "Basic ${basic}")
    conn.setRequestProperty("JWT-Token", jwt)
    conn.setRequestProperty("CDCSignature", signature)

    int rc = conn.responseCode
    def stream  = (rc >= 200 && rc < 300) ? conn.inputStream : conn.errorStream
    String resp = stream?.getText("UTF-8") ?: ''

    log?.addAttachmentAsString("CDC-Response-${profileId}", resp, "application/json")

    if(rc != 200){
        throw new RuntimeException("CDC-Aufruf fehlgeschlagen. HTTP-RC: ${rc}")
    }
    return new JsonSlurper().parseText(resp)
}

/*-------------------------------------------------------------------------*/
/*  validateProfile – Prüft Business-Regeln                                */
/*-------------------------------------------------------------------------*/
void validateProfile(Map prof){
    if(prof?.isRegistered != true){
        throw new RuntimeException("Profil nicht registriert (isRegistered != true).")
    }
    if(!prof.profile){
        throw new RuntimeException("Profilobjekt fehlt.")
    }
    if(!prof.profile.lastName){
        throw new RuntimeException("lastName fehlt.")
    }
}

/*-------------------------------------------------------------------------*/
/*  mapToMDM – Erstellt XML gem. Zielschema                                */
/*-------------------------------------------------------------------------*/
String mapToMDM(Map profileMap, def log){
    def prof      = profileMap.profile
    def emailsArr = profileMap.emails?.verified ?: []

    /* UUID für MessageHeader                                                */
    String uuid          = UUID.randomUUID().toString()
    String idNoDash      = uuid.replaceAll('-', '')

    /* XML bauen                                                             */
    StringWriter sw = new StringWriter()
    def bp = new MarkupBuilder(sw)
    bp.bp.BusinessPartnerSUITEBulkReplicateRequest('xmlns:bp':'urn:sap-com:document:sap:rfc:functions'){
        BusinessPartnerSUITEReplicateRequestMessage{
            MessageHeader{
                UUID(uuid)
                ID(idNoDash)
            }
            BusinessPartner{
                Common{
                    KeyWordsText(prof.firstName)
                    AdditionalKeyWordsText(prof.lastName)
                    Person{
                        Name{
                            GivenName(prof.firstName)
                            FamilyName(prof.lastName)
                        }
                    }
                }
                AddressIndependentCommInfo{
                    emailsArr.each{ emObj ->
                        if(emObj?.email){
                            Email{
                                URI(emObj.email)
                            }
                        }
                    }
                }
            }
        }
    }
    String xmlOut = sw.toString()
    log?.addAttachmentAsString("MDM-Payload", xmlOut, "application/xml")
    return xmlOut
}

/*-------------------------------------------------------------------------*/
/*  replicateToMDM – HTTP-POST an SAP MDM                                   */
/*-------------------------------------------------------------------------*/
void replicateToMDM(Map cfg, String profileId, String xmlPayload, def log){
    String urlStr = "${cfg.requestURLMDM}/${profileId}"
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.requestMethod = "POST"
    conn.doOutput      = true

    String basic = "${cfg.requestUserMDM}:${cfg.requestPasswordMDM}".bytes.encodeBase64().toString()
    conn.setRequestProperty("Authorization", "Basic ${basic}")
    conn.setRequestProperty("Content-Type", "application/xml; charset=UTF-8")

    conn.outputStream.withWriter("UTF-8"){ it << xmlPayload }
    int rc = conn.responseCode
    if(rc < 200 || rc >= 300){
        String err = conn.errorStream?.getText("UTF-8")
        log?.addAttachmentAsString("MDM-Error-${profileId}", err ?: '', "text/plain")
        throw new RuntimeException("MDM-Replikation fehlgeschlagen. HTTP-RC: ${rc}")
    }
    log?.setStringProperty("MDM-ResponseCode", rc.toString())
}

/*-------------------------------------------------------------------------*/
/*  handleError – Einheitliches Error-Handling                             */
/*-------------------------------------------------------------------------*/
def handleError(String body, Exception e, def messageLog) {
    /* Payload als Attachment anhängen                                       */
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/plain")
    def errorMsg = "Fehler im Integrations-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}