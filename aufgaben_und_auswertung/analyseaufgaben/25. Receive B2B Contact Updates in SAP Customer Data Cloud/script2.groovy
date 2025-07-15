/****************************************************************************************
* Skriptname :  BP2CDC_ProfileUpdate.groovy                                              *
* Autor      :  AI Assistant                                                             *
* Version    :  1.0                                                                      *
* Beschreibung:                                                                          *
*   Repliziert B2B-Contact Updates von SAP MDI in die SAP Customer Data Cloud.           *
*   –  Liest BusinessPartner-Datensätze aus dem XML-Payload                              *
*   –  Ruft für jede UID das bestehende CDC-Profil ab                                    *
*   –  Vergleicht eingehende Daten mit vorhandenem Profil                                *
*   –  Baut nur die geänderten Felder zusammen und aktualisiert das Profil               *
*                                                                                        *
* Modularer Aufbau (gemäß Aufgabenstellung):                                             *
*   • initContext(...)        – Properties & Header befüllen                             *
*   • createJwtToken(...)     – JWT erzeugen                                             *
*   • signToken(...)          – JWT signieren                                            *
*   • buildProfile(...)       – XML->JSON Mapping                                        *
*   • mergeProfiles(...)      – Differenzbildung/Validierung                             *
*   • buildRequestBody(...)   – Request-Body für POST erstellen                          *
*   • getAccount(...)         – API-Call “Get Account”                                   *
*   • updateProfile(...)      – API-Call “Update Profile”                                *
*   • handleError(...)        – Zentrales Error-Handling                                 *
*****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.Instant
import java.net.URLEncoder
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/*-----------------------------------------------------------------------------
 * Zentrales Error-Handling
 * Fügt den fehlerhaften Payload als Attachment an und wirft RuntimeException
 *---------------------------------------------------------------------------*/
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "", "text/plain")
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

/*-----------------------------------------------------------------------------
 * Liest Properties / Headers aus dem Message-Objekt und setzt Platzhalter,
 * falls Werte fehlen. Gibt Map mit allen relevanten Werten zurück.
 *---------------------------------------------------------------------------*/
def initContext(Message message, def messageLog){
    def p = message.getProperties()
    [
        requestUser : p.requestUser  ?: "placeholder",
        requestPassword : p.requestPassword ?: "placeholder",
        requestURL : p.requestURL ?: "placeholder",
        hmacKey : p.plainHMACKeyForSignature ?: "placeholder",
        apiKey  : p.apiKey ?: "placeholder"
    ]
}

/* JWT-Hilfsfunktion – liefert Base64Url-String ohne Padding */
def b64Url(byte[] data){
    Base64.getUrlEncoder().withoutPadding().encodeToString(data)
}

/*-----------------------------------------------------------------------------
 * Erstellt JWT-Token (Header + Claim) als Base64Url kodierten String
 *---------------------------------------------------------------------------*/
def createJwtToken(String apiKey){
    def headerJson = JsonOutput.toJson([alg:"RS256", typ:"JWT", kid:apiKey])
    def claimJson  = JsonOutput.toJson([iat:Instant.now().epochSecond])
    "${b64Url(headerJson.bytes)}.${b64Url(claimJson.bytes)}"
}

/*-----------------------------------------------------------------------------
 * Signiert den JWT-Token mittels HmacSHA256
 *---------------------------------------------------------------------------*/
def signToken(String token, String key){
    Mac mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key.bytes, "HmacSHA256"))
    Base64.encoder.encodeToString(mac.doFinal(token.bytes))
}

/*-----------------------------------------------------------------------------
 * Mapping BusinessPartner-XML -> Profil-Map
 *---------------------------------------------------------------------------*/
def buildProfile(bp){
    def m = [:]
    m.firstName = bp?.Name?.FirstName?.text()
    m.lastName  = bp?.Name?.LastName?.text()
    m.city      = bp?.Address?.CityName?.text()
    m.findAll{ k,v -> v }                       // Null-Einträge entfernen
}

/*-----------------------------------------------------------------------------
 * Vergleicht eingehende mit bestehendem Profil
 * – liefert gemergtes Profil
 * – null, falls keine Änderungen
 *---------------------------------------------------------------------------*/
def mergeProfiles(existing, incoming){
    if(existing == null){ return incoming }     // komplett neuer Datensatz
    def diff = [:]
    incoming.each{ k,v -> if(existing[k] != v){ diff[k] = v } }
    diff.isEmpty() ? null : existing + diff
}

/*-----------------------------------------------------------------------------
 * Baut Request-Body-String für den POST-Aufruf
 *---------------------------------------------------------------------------*/
def buildRequestBody(String apiKey, String uid, Map profile){
    def prJson = JsonOutput.toJson(profile)
    "apiKey=${URLEncoder.encode(apiKey,'UTF-8')}" +
    "&UID=${URLEncoder.encode(uid,'UTF-8')}" +
    "&profile=${URLEncoder.encode(prJson,'UTF-8')}"
}

/*-----------------------------------------------------------------------------
 * API-Call 1 – GET Account
 *---------------------------------------------------------------------------*/
def getAccount(ctx, String uid, String jwt, String sig, def log){
    def url  = new URL("${ctx.requestURL}/${uid}")
    def conn = url.openConnection()
    conn.requestMethod = "GET"
    conn.connectTimeout = conn.readTimeout = 15000
    conn.setRequestProperty("Authorization",
        "Basic " + "${ctx.requestUser}:${ctx.requestPassword}".bytes.encodeBase64())
    conn.setRequestProperty("JWT-Token",    jwt)
    conn.setRequestProperty("CDCSignature", sig)

    int rc = conn.responseCode
    log?.addAttachmentAsString("GetAccount_RC_${uid}", "$rc", "text/plain")
    if(rc == 200){
        new JsonSlurper().parse(conn.inputStream)?.profile
    }else if(rc == 404){
        null                                            // Profil nicht vorhanden
    }else{
        throw new RuntimeException("GET Account fehlgeschlagen (UID $uid, RC $rc)")
    }
}

/*-----------------------------------------------------------------------------
 * API-Call 2 – POST Update Profile
 *---------------------------------------------------------------------------*/
def updateProfile(ctx, String uid, String jwt, String sig,
                  String bodyStr, def log){
    def url  = new URL("${ctx.requestURL}/${uid}")
    def conn = url.openConnection()
    conn.requestMethod = "POST"
    conn.doOutput      = true
    conn.connectTimeout = conn.readTimeout = 15000
    conn.setRequestProperty("Content-Type",
        "application/x-www-form-urlencoded; charset=UTF-8")
    conn.setRequestProperty("Authorization",
        "Basic " + "${ctx.requestUser}:${ctx.requestPassword}".bytes.encodeBase64())
    conn.setRequestProperty("JWT-Token",    jwt)
    conn.setRequestProperty("CDCSignature", sig)

    conn.outputStream.withWriter("UTF-8"){ it << bodyStr }
    int rc = conn.responseCode
    log?.addAttachmentAsString("UpdateProfile_RC_${uid}", "$rc", "text/plain")
    if(rc != 200){
        throw new RuntimeException("Update Profile fehlgeschlagen (UID $uid, RC $rc)")
    }
}

/*-----------------------------------------------------------------------------
 * MAIN
 *---------------------------------------------------------------------------*/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try{
        /* 1. Kontext vorbereiten */
        def ctx     = initContext(message, messageLog)
        def xmlBody = message.getBody(String) ?: ""
        def root    = new XmlSlurper().parseText(xmlBody)
        def partners= root.BusinessPartner
        if(!partners){ throw new RuntimeException("Keine BusinessPartner gefunden.") }

        /* 2. Für jeden BusinessPartner … */
        partners.each{ bp ->

            /* 2.1 UID ermitteln */
            def uid = bp?.Common?.IAMID?.text()
            if(!uid){ throw new RuntimeException("Fehlende IAMID im BusinessPartner.") }

            /* 2.2 JWT & Signatur pro Request */
            def jwt = createJwtToken(ctx.apiKey)
            def sig = signToken(jwt, ctx.hmacKey)

            /* 2.3 GET Account */
            def existingProfile = getAccount(ctx, uid, jwt, sig, messageLog)

            /* 2.4 Profil-Mapping & Vergleich */
            def newProfile     = buildProfile(bp)
            def profileToSend  = mergeProfiles(existingProfile, newProfile)
            if(profileToSend == null){
                throw new RuntimeException("Keine Änderungen für UID $uid erkannt.")
            }

            /* 2.5 POST Update Profile */
            def bodyStr = buildRequestBody(ctx.apiKey, uid, profileToSend)
            updateProfile(ctx, uid, jwt, sig, bodyStr, messageLog)
        }

    }catch(Exception e){
        handleError(message.getBody(String) as String, e, messageLog)
    }
    return message
}