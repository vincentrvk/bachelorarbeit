/*****************************************************************************************
 * SAP Cloud Integration – Groovy-Skript
 *
 * Repliziert B2C-Contact-Updates aus SAP Master Data Integration (MDI) in die
 * SAP Customer Data Cloud (CDC).
 *
 * Die Implementierung folgt streng den vorgegebenen Anforderungen:
 *  • Vollständig modulare Struktur
 *  • Ausführliche deutschsprachige Funktions-Kommentare
 *  • Umfassendes Error-Handling inkl. Payload-Weitergabe als Attachment
 *  • Keine globalen Konstanten / Variablen
 *  • Kein unnötiger Import
 *****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import java.net.URLEncoder
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import groovy.json.*

// ======================================================
// Einstiegspunkt der Groovy-Script-Step
// ======================================================
Message processData(Message message) {

    // MessageLog für Monitoring
    def msgLog = messageLogFactory.getMessageLog(message)

    try {
        /* ------------------------------------------------
         * 1. Header / Property-Handling
         * ------------------------------------------------*/
        def context          = readContextValues(message)

        /* ------------------------------------------------
         * 2. BusinessPartner laden (pro Split-Iteration)
         * ------------------------------------------------*/
        def bp               = extractBusinessPartner(message.getBody(String) as String)

        /* ------------------------------------------------
         * 3. JWT & Signatur erzeugen
         * ------------------------------------------------*/
        def jwt              = buildJwtToken(context.apiKey)
        def signature        = signJwtToken(jwt, context.hmacKey)

        /* ------------------------------------------------
         * 4. GET Account aus CDC lesen
         * ------------------------------------------------*/
        def getResponse      = callGetAccount(
                                    "${context.requestURL}/${context.role}/${context.profileID}",
                                    jwt, signature, context.user, context.password)

        /* ------------------------------------------------
         * 5. Profil aus Antwort filtern
         * ------------------------------------------------*/
        def existingProfile  = filterProfile(getResponse.body)

        /* ------------------------------------------------
         * 6. Profil-Vergleich & Merging
         * ------------------------------------------------*/
        def updatedProfile   = mergeProfiles(bp, existingProfile)

        /* ------------------------------------------------
         * 7. Request-Payload für Update bauen
         * ------------------------------------------------*/
        def postBody         = buildUpdatePayload(context.apiKey, context.profileID, updatedProfile)

        /* ------------------------------------------------
         * 8. CDC-Profil aktualisieren
         * ------------------------------------------------*/
        callUpdateProfile(
                "${context.requestURL}/${context.profileID}",
                postBody, jwt, signature, context.user, context.password)

        // alles fertig → Response-Payload leer lassen
        message.setBody("")
        return message

    } catch (Exception e) {
        return handleError(message, msgLog, e)
    }
}

// ######################################################
// Modul-Funktionen
// ######################################################

/**
 * Liest relevante Header/Properties und legt Default-Werte fest.
 */
private static Map readContextValues(Message msg) {
    def getter       = { String name -> msg.getProperty(name) ?: msg.getHeader(name, String) ?: "placeholder" }

    return [
        user      : getter("requestUser"),
        password  : getter("requestPassword"),
        requestURL: getter("requestURL"),
        hmacKey   : getter("plainHMACKeyForSignature"),
        apiKey    : getter("apiKey"),
        // Die folgenden Werte werden erst aus dem XML extrahiert
        profileID : "", role: ""
    ]
}

/**
 * Extrahiert einen BusinessPartner aus dem XML-Payload.
 * Liefert zusätzlich profileID & role zurück und schreibt sie in die Message-Properties.
 */
private static Map extractBusinessPartner(String xmlPayload) {
    def xml        = new XmlSlurper().parseText(xmlPayload)
    def bpNode     = xml.'**'.find { it.name() == 'BusinessPartner' }   // erwartet einziges Element

    def profileID  = bpNode?.Common?.IAMID?.text()      ?: ""
    def roleCode   = bpNode?.Role?.RoleCode?.text()     ?: ""

    // Minimale BP-Daten für Vergleich
    def bpMap      = [
        firstName : bpNode?.Name?.FirstName?.text(),
        lastName  : bpNode?.Name?.LastName?.text(),
        city      : bpNode?.Address?.City?.text()
    ]

    // Properties im Message-Kontext ablegen
    bpNode?.document.properties?.with {
        it.setProperty("profileID", profileID)
        it.setProperty("role",      roleCode)
    }

    return [bp: bpMap, profileID: profileID, role: roleCode]
}

/**
 * Baut einen signaturfreien JWT bestehend aus Header & Claim (iat-Zeitstempel) im URL-Safe-Base64-Format.
 */
private static String buildJwtToken(String apiKey) {
    def b64Url  = { String txt ->
        return txt.bytes.encodeBase64().toString()
                  .replace('+','-').replace('/','_').replaceAll('=','')
    }

    def header  = b64Url(JsonOutput.toJson([alg:"RS256", typ:"JWT", kid: apiKey]))
    def claim   = b64Url(JsonOutput.toJson([iat: (System.currentTimeMillis()/1000 as long)]))

    return "${header}.${claim}"
}

/**
 * Signiert den JWT-Token mit HmacSHA256.
 */
private static String signJwtToken(String token, String secret) {
    Mac mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"))
    return mac.doFinal(token.getBytes("UTF-8")).encodeBase64().toString()
}

/**
 * Führt den GET-Aufruf „Get Account“ gegen CDC aus.
 */
private static Map callGetAccount(String urlStr, String jwt, String sig,
                                  String user, String pwd) {

    def url  = new URL(urlStr)
    def conn = (HttpURLConnection) url.openConnection()

    conn.requestMethod = "GET"
    conn.setRequestProperty("Authorization",
            "Basic ${(user+':'+pwd).bytes.encodeBase64().toString()}")
    conn.setRequestProperty("JWT-Token",    jwt)
    conn.setRequestProperty("CDCSignature", sig)

    conn.connect()
    int  rc   = conn.responseCode
    String body = (rc >= 200 && rc < 300) ? conn.inputStream.text : (conn.errorStream?.text ?: "")

    if(rc == 404) {      // Profil nicht vorhanden → leere Map zurück
        return [status: rc, body:"{}"]
    }
    if(rc != 200) {
        throw new RuntimeException("CDC-GET-Request schlug fehl (HTTP ${rc}) – Body: ${body}")
    }
    return [status: rc, body: body]
}

/**
 * Reduziert die CDC-Antwort auf den reinen „profile“-Teil.
 */
private static Map filterProfile(String responseJson) {
    if(!responseJson?.trim()) { return [:] }

    def json = new JsonSlurper().parseText(responseJson)

    // Prospect-Antwort enthält „results[...].profile“
    if(json?.results instanceof List) {
        def p = json.results.find { it.profile != null }?.profile
        return p ?: [:]
    }
    // Customer-Antwort enthält das Profil direkt
    if(json?.profile) { return json.profile as Map }

    return [:]    // nichts gefunden
}

/**
 * Vergleicht bestehendes Profil mit eingehenden BP-Daten und mapt nur die Änderungen.
 * Liefert das zusammengeführte Profil zurück oder wirft Exception, wenn keine Änderungen.
 */
private static Map mergeProfiles(Map bpDataWithWrapper, Map existingProfile) {

    def bpData = bpDataWithWrapper.bp

    // Wenn Profil nicht existiert, übernehmen wir direkt alle BP-Felder
    if(existingProfile == null || existingProfile.isEmpty()) {
        return bpData
    }

    // Feld-weise Abgleich
    def updated = [:] + existingProfile
    def change  = false

    bpData.each { k,v ->
        if(v && v != existingProfile[k]) {
            updated[k] = v
            change = true
        }
    }

    if(!change) {
        throw new RuntimeException("Keine Änderungen zum bestehenden Profil festgestellt – Update abgebrochen.")
    }
    return updated
}

/**
 * Baut den x-www-form-urlencoded Payload für das CDC-Update.
 */
private static String buildUpdatePayload(String apiKey, String uid, Map profile) {

    def enc = { v -> URLEncoder.encode(v as String, "UTF-8") }

    return  "apiKey=${enc(apiKey)}" +
            "&UID=${enc(uid)}" +
            "&profile=${enc(JsonOutput.toJson(profile))}"
}

/**
 * Führt POST-Update gegen CDC aus.
 */
private static void callUpdateProfile(String urlStr, String body, String jwt,
                                      String sig, String user, String pwd) {

    def url  = new URL(urlStr)
    def conn = (HttpURLConnection) url.openConnection()

    conn.requestMethod = "POST"
    conn.doOutput      = true
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
    conn.setRequestProperty("Authorization",
            "Basic ${(user+':'+pwd).bytes.encodeBase64().toString()}")
    conn.setRequestProperty("JWT-Token",    jwt)
    conn.setRequestProperty("CDCSignature", sig)

    conn.outputStream.withWriter("UTF-8") { it << body }
    conn.connect()

    int rc = conn.responseCode
    if(rc != 200) {
        String err = conn.errorStream?.text ?: ""
        throw new RuntimeException("CDC-POST-Request schlug fehl (HTTP ${rc}) – Body: ${err}")
    }
}

/**
 * Zentrales Error-Handling – schreibt Payload-Attachment & wirft Exception neu.
 */
private static Message handleError(Message message, def messageLog, Exception e) {
    def payload = message.getBody(String) ?: "<no-body/>"
    messageLog?.addAttachmentAsString("ErrorPayload", payload, "text/plain")
    def errMsg  = "Fehler im CDC-Integrationsskript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}