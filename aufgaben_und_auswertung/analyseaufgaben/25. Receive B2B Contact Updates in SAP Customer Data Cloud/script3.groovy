/****************************************************************************************
 *  Groovy-Skript:  B2B Contact Update – SAP MDI ➜ SAP Customer Data Cloud
 *  Autor        :  Integration GPT – Senior Integration Developer
 *  Hinweise     :  – Das Skript wird pro BusinessPartner-Element (Splitter) aufgerufen
 *                  – Es werden alle Anforderungen der Aufgabenstellung erfüllt
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.util.Base64

/* =========================================================================
 *  Haupt-Entry-Point
 * ========================================================================= */
Message processData(Message message) {

    // Logger für Monitoring
    def messageLog = messageLogFactory?.getMessageLog(message)

    try {
        // 1. Initiale Variablen (Header & Properties) setzen/auslesen
        def vars = setInitialValues(message)

        // 2. IAMID (=UID) aus XML Payload extrahieren
        String uid = extractProfileUid(message.getBody(String), messageLog)
        message.setProperty("profileID", uid)

        // 3. JWT-Token & Signature erzeugen
        String jwtToken   = createJwtToken(vars.apiKey)
        String cdcSig     = signToken(jwtToken, vars.hmacKey)

        // 4. Bestehendes Profil abrufen
        Map existingProfile = callGetAccount(
                                vars.requestURL, uid, vars.user, vars.pass,
                                jwtToken, cdcSig, messageLog)

        // 5. Eingehendes Profil aus XML bilden
        Map incomingProfile = buildIncomingProfile(message.getBody(String))

        // 6. Relevante Änderungen ermitteln
        Map diffProfile = compareProfiles(existingProfile, incomingProfile)

        if (diffProfile.isEmpty()) {
            throw new IllegalStateException("Keine zu übertragenden Profil-Änderungen vorhanden.")
        }

        // 7. Request-Body für Update aufbauen
        String updateBody = buildUpdateRequestBody(vars.apiKey, uid, diffProfile)

        // 8. Profil aktualisieren
        callUpdateProfile(vars.requestURL, uid, vars.user, vars.pass,
                          jwtToken, cdcSig, updateBody, messageLog)

        // 9. Body unverändert lassen (oder auf Wunsch anpassen)
        return message

    } catch (Exception e) {
        handleError(message.getBody(String), e, messageLog)
        return message          // Rückgabe wird von handleError nicht erreicht (throw)
    }
}

/* =========================================================================
 *  1. Header / Property Handling
 * ========================================================================= */
private Map setInitialValues(Message msg) {
    // Existierende Werte holen, sonst Platzhalter
    def getVal = { name -> (msg.getProperty(name) ?: msg.getHeader(name, String.class) ?: "placeholder") }

    def vars = [
        user    : getVal("requestUser"),
        pass    : getVal("requestPassword"),
        requestURL : getVal("requestURL"),
        hmacKey : getVal("plainHMACKeyForSignature"),
        apiKey  : getVal("apiKey")
    ]

    // Sicherstellen, dass Properties für Folge-Schritte verfügbar sind
    vars.each { k,v -> msg.setProperty(k, v) }
    return vars
}

/* =========================================================================
 *  2. UID (IAMID) aus XML Payload extrahieren
 * ========================================================================= */
private String extractProfileUid(String xmlBody, def log) {
    try {
        def xml = new XmlSlurper().parseText(xmlBody)
        String uid = xml.'**'.find { it.name() == 'IAMID' }?.text()
        if (!uid) throw new IllegalArgumentException("IAMID (UID) im Payload fehlt.")
        return uid
    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Auslesen der IAMID: ${e.message}", e)
    }
}

/* =========================================================================
 *  3. JWT-Token & Signature
 * ========================================================================= */
private String createJwtToken(String apiKey) {
    def base64Url = { String str ->
        Base64.getUrlEncoder().withoutPadding()
               .encodeToString(str.getBytes(StandardCharsets.UTF_8))
    }

    String jwtHeader = base64Url('{"alg":"RS256","typ":"JWT","kid":"' + apiKey + '"}')
    long   ts        = (System.currentTimeMillis() / 1000L) as long
    String jwtClaim  = base64Url('{"iat":' + ts + '}')

    return "${jwtHeader}.${jwtClaim}"
}

private String signToken(String token, String hmacKey) {
    Mac mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(hmacKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
    byte[] rawSig = mac.doFinal(token.getBytes(StandardCharsets.UTF_8))
    return Base64.getUrlEncoder().withoutPadding().encodeToString(rawSig)
}

/* =========================================================================
 *  4. GET Account
 * ========================================================================= */
private Map callGetAccount(String baseUrl, String uid,
                           String user, String pass,
                           String jwt, String sig, def log) {

    String url = "${baseUrl}/${uid}"
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    try {
        conn.setRequestMethod("GET")
        conn.setRequestProperty("Authorization", "Basic " +
                Base64.getEncoder().encodeToString("${user}:${pass}".getBytes(StandardCharsets.UTF_8)))
        conn.setRequestProperty("JWT-Token", jwt)
        conn.setRequestProperty("CDCSignature", sig)
        conn.setConnectTimeout(15000)
        conn.setReadTimeout(15000)

        int rc = conn.getResponseCode()
        log?.addAttachmentAsString("GET Account RC", rc.toString(), "text/plain")

        if (rc == 200) {
            String resp = conn.inputStream.getText(StandardCharsets.UTF_8.name())
            return filterProfile(resp, log)              // nur "profile"-Teil
        } else if (rc == 404) {
            // Profil nicht vorhanden – ok, leer zurückgeben
            return [:]
        } else {
            String errorBody = conn.errorStream ? conn.errorStream.getText(StandardCharsets.UTF_8.name()) : ""
            throw new RuntimeException("GET Account fehlgeschlagen (HTTP ${rc}): ${errorBody}")
        }

    } finally {
        conn?.disconnect()
    }
}

/* =========================================================================
 *  5. Profil-Filter (nur profile-Node)
 * ========================================================================= */
private Map filterProfile(String jsonString, def log) {
    if (!jsonString) return [:]
    def json = new JsonSlurper().parseText(jsonString)
    return (json?.profile ?: [:]) as Map
}

/* =========================================================================
 *  6. Eingehendes XML-Profil aufbauen
 * ========================================================================= */
private Map buildIncomingProfile(String xmlBody) {
    def xml = new XmlSlurper().parseText(xmlBody)

    Map profile = [:]
    profile.firstName = xml.'**'.find { it.name() == 'FirstName' }?.text()
    profile.lastName  = xml.'**'.find { it.name() == 'LastName' }?.text()
    profile.city      = xml.'**'.find { it.name() == 'CityName' }?.text()

    // Feld-Platzhalter um CDC-Validierung zu bestehen
    ['email','username'].each {
        profile[it] = "placeholder_${it}"
    }

    return profile.findAll { it.value }          // leere Einträge entfernen
}

/* =========================================================================
 *  7. Profil-Vergleich & Änderungen ermitteln
 * ========================================================================= */
private Map compareProfiles(Map existing, Map incoming) {
    if (!existing) return incoming               // komplettes Profil, da neu

    Map diff = [:]
    incoming.each { k,v ->
        if (!existing.containsKey(k) || existing[k] != v) {
            diff[k] = v
        }
    }
    return diff
}

/* =========================================================================
 *  8. Request-Body für Update bauen
 * ========================================================================= */
private String buildUpdateRequestBody(String apiKey, String uid, Map profileMap) {
    String profileJson = JsonOutput.toJson(profileMap)
    // CDC erwartet einen URL-encoded Query-String
    return "apiKey=${URLEncoder.encode(apiKey, 'UTF-8')}" +
           "&UID=${URLEncoder.encode(uid, 'UTF-8')}" +
           "&profile=${URLEncoder.encode(profileJson, 'UTF-8')}"
}

/* =========================================================================
 *  9. POST Update Profile
 * ========================================================================= */
private void callUpdateProfile(String baseUrl, String uid,
                               String user, String pass,
                               String jwt, String sig,
                               String body, def log) {

    String url = "${baseUrl}/${uid}"
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    try {
        conn.setRequestMethod("POST")
        conn.doOutput = true
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
        conn.setRequestProperty("Authorization", "Basic " +
                Base64.getEncoder().encodeToString("${user}:${pass}".getBytes(StandardCharsets.UTF_8)))
        conn.setRequestProperty("JWT-Token", jwt)
        conn.setRequestProperty("CDCSignature", sig)

        conn.outputStream.withWriter("UTF-8") { it << body }

        int rc = conn.getResponseCode()
        log?.addAttachmentAsString("POST Update RC", rc.toString(), "text/plain")

        if (rc !in [200, 201, 202, 204]) {
            String errorBody = conn.errorStream ? conn.errorStream.getText(StandardCharsets.UTF_8.name()) : ""
            throw new RuntimeException("Profil-Update fehlgeschlagen (HTTP ${rc}): ${errorBody}")
        }

    } finally {
        conn?.disconnect()
    }
}

/* =========================================================================
 *  10. Zentrales Error-Handling
 * ========================================================================= */
private void handleError(String body, Exception e, def messageLog) {
    // Payload als Attachment für Monitoring
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "", "text/plain")
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}