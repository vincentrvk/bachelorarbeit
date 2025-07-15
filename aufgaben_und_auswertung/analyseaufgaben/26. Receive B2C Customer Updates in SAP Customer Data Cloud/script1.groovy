/******************************************************************************************
 *  SAP Cloud Integration – Groovy-Script
 *  Aufgabe: Replikation von B2C-Kontaktupdates nach SAP Customer Data Cloud (CDC)
 *  Autor  : AI Assistant (Senior Integration Entwickler)
 *
 *  WICHTIG:
 *  1. Das Skript ist modular aufgebaut – jede wesentliche Aufgabe ist in einer eigenen
 *     Methode gekapselt.
 *  2. Alle Methoden sind deutsch kommentiert.
 *  3. Umfassendes Error-Handling: Bei jeder Exception wird der ursprüngliche Payload als
 *     Attachment weitergereicht und eine aussagekräftige Fehlermeldung geworfen.
 ******************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.util.Base64
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.*

/* Haupteinstiegspunkt für SAP CI */
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        /* 1. Header & Property Handling ********************************************************/
        def ctx                     = prepareContext(message)     // Map mit allen benötigten Kontextwerten
        def bodyString              = message.getBody(String)     // XML als String
        ctx.body                    = bodyString                  // merken für Error-Handling

        /* 2. XML parsen & Profilinformationen extrahieren *************************************/
        def xml                     = new XmlSlurper().parseText(bodyString)
        ctx.profileId               = getXmlValue(xml, 'Common/IAMID')
        ctx.role                    = getXmlValue(xml, 'Role/RoleCode')
        def incomingProfileMap      = buildIncomingProfile(xml)

        /* 3. JWT‐Token & Signatur erstellen ***************************************************/
        ctx.jwtToken                = buildJwtToken(ctx.apiKey)
        ctx.cdcSignature            = signJwtToken(ctx.jwtToken, ctx.plainHMACKeyForSignature)

        /* 4. GET Account Call (CDC) ***********************************************************/
        Map existingProfileMap      = getAccount(ctx)

        /* 5. Profile vergleichen **************************************************************/
        def comparisonResult        = compareProfiles(incomingProfileMap, existingProfileMap)
        if (!comparisonResult.hasChanges) {
            throw new RuntimeException("Kein CDC-Update erforderlich – Profile identisch.")
        }

        /* 6. POST Update Profile **************************************************************/
        postUpdateProfile(ctx, comparisonResult.updatedProfile)

        /* 7. (Optional) Response in Message Body setzen ***************************************/
        message.setBody("Profil ${ctx.profileId} wurde erfolgreich in der CDC aktualisiert.")
        return message

    } catch (Exception ex) {
        handleError(message, ex, messageLog)
    }
}

/************************************************************************************************
 *                                       HILFSMETHODEN
 ************************************************************************************************/
/* Kontext (Header & Properties) vorbereiten */
private Map prepareContext(Message message) {
    Map ctx = [:]

    /* Properties aus IFlow oder Fallback auf 'placeholder' */
    ctx.requestUser                = message.getProperty('requestUser')               ?: 'placeholder'
    ctx.requestPassword            = message.getProperty('requestPassword')           ?: 'placeholder'
    ctx.requestURL                 = message.getProperty('requestURL')                ?: 'placeholder'
    ctx.plainHMACKeyForSignature   = message.getProperty('plainHMACKeyForSignature')  ?: 'placeholder'
    ctx.apiKey                     = message.getProperty('apiKey')                    ?: 'placeholder'

    /* Header-Werte (falls vorhanden) */
    ctx.requestUser     = message.getHeader('requestUser',     String) ?: ctx.requestUser
    ctx.requestPassword = message.getHeader('requestPassword', String) ?: ctx.requestPassword
    ctx.requestURL      = message.getHeader('requestURL',      String) ?: ctx.requestURL

    return ctx
}

/* Wert aus XML via Pfad holen */
private String getXmlValue(def xml, String path) {
    def parts = path.tokenize('/')
    def node  = parts.inject(xml) { n, p -> n?."$p" }
    return node?.text()?.trim()
}

/* Incoming Profil (aus MDI-XML) als Map aufbauen */
private Map buildIncomingProfile(def xml) {
    [
        firstName : getXmlValue(xml, 'Name/FirstName'),
        lastName  : getXmlValue(xml, 'Name/LastName'),
        city      : getXmlValue(xml, 'Address/City')
    ].findAll { k, v -> v }                                 // entferne leere Felder
}

/* JWT-Token (<header>.<claim>) erstellen */
private String buildJwtToken(String apiKey) {
    String headerJson   = JsonOutput.toJson([alg:'RS256', typ:'JWT', kid:apiKey])
    String claimJson    = JsonOutput.toJson([iat: (System.currentTimeMillis() / 1000).longValue()])
    String headerB64    = Base64.encoder.encodeToString(headerJson.getBytes(StandardCharsets.UTF_8))
    String claimB64     = Base64.encoder.encodeToString(claimJson.getBytes(StandardCharsets.UTF_8))
    return "${headerB64}.${claimB64}"
}

/* HMAC-SHA256 Signatur für JWT-Token berechnen */
private String signJwtToken(String jwtToken, String secretKey) {
    Mac mac             = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
    byte[] rawHmac      = mac.doFinal(jwtToken.getBytes(StandardCharsets.UTF_8))
    return Base64.encoder.encodeToString(rawHmac)
}

/* GET Account Call an CDC */
private Map getAccount(Map ctx) {
    String urlStr   = "${ctx.requestURL}/${ctx.role}/${ctx.profileId}"
    HttpURLConnection con = (HttpURLConnection) new URL(urlStr).openConnection()
    con.setRequestMethod("GET")
    con.setRequestProperty("Authorization", "Basic " + "${ctx.requestUser}:${ctx.requestPassword}".bytes.encodeBase64().toString())
    con.setRequestProperty("JWT-Token", ctx.jwtToken)
    con.setRequestProperty("CDCSignature", ctx.cdcSignature)

    int rc = con.responseCode
    if (rc == 200) {
        String responseBody = con.inputStream.getText("UTF-8")
        def json            = new JsonSlurper().parseText(responseBody)
        /* Nur den 'profile' Teil zurückgeben, wenn vorhanden */
        if (json?.profile) {
            return json.profile as Map
        }
        /* Prospect Antwort => results[?].profile */
        if (json?.results) {
            def profileEntry = json.results.find { it.profile }?.profile
            return profileEntry ? profileEntry as Map : [:]
        }
        return [:]
    }
    if (rc == 404) {                      // Profil nicht vorhanden – leere Map zurück
        return [:]
    }
    throw new RuntimeException("GET Account CDC fehlgeschlagen (HTTP ${rc}).")
}

/* Profile vergleichen – Ergebnis: Map(hasChanges, updatedProfile) */
private Map compareProfiles(Map incoming, Map existing) {
    boolean hasChanges   = false
    Map merged           = [:]

    incoming.each { k, v ->
        def oldVal = existing[k]
        if (oldVal == null || oldVal.toString() != v.toString()) {
            hasChanges   = true
            merged[k]    = v
        } else {
            merged[k]    = oldVal                        // unverändert übernehmen
        }
    }
    /* Füge restliche Felder aus bestehendem Profil hinzu */
    existing.each { k, v -> if (!merged.containsKey(k)) merged[k] = v }

    return [hasChanges: hasChanges, updatedProfile: merged]
}

/* POST Update Profile Call */
private void postUpdateProfile(Map ctx, Map updatedProfile) {
    String profileJson   = JsonOutput.toJson(updatedProfile)
    String payload       = "apiKey=${URLEncoder.encode(ctx.apiKey, 'UTF-8')}" +
                           "&UID=${URLEncoder.encode(ctx.profileId, 'UTF-8')}" +
                           "&profile=${URLEncoder.encode(profileJson, 'UTF-8')}"

    String urlStr        = "${ctx.requestURL}/${ctx.profileId}"
    HttpURLConnection con = (HttpURLConnection) new URL(urlStr).openConnection()
    con.setRequestMethod("POST")
    con.doOutput         = true
    con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
    con.setRequestProperty("Authorization", "Basic " + "${ctx.requestUser}:${ctx.requestPassword}".bytes.encodeBase64().toString())
    con.setRequestProperty("JWT-Token", ctx.jwtToken)
    con.setRequestProperty("CDCSignature", ctx.cdcSignature)

    con.outputStream.withWriter("UTF-8") { it << payload }
    int rc = con.responseCode
    if (rc != 200 && rc != 201) {
        throw new RuntimeException("POST UpdateProfile CDC fehlgeschlagen (HTTP ${rc}).")
    }
}

/* Zentrales Error-Handling: Payload als Attachment + aussagekräftige Fehlermeldung */
private void handleError(Message message, Exception ex, def messageLog) {
    try {
        messageLog?.addAttachmentAsString("ErrorPayload", (message.getBody(String) ?: ""), "text/plain")
    } catch (Exception ignore) { /* Attachment Fehler ignorieren */ }
    throw new RuntimeException("Fehler im CDC-Integrationsskript: ${ex.message}", ex)
}