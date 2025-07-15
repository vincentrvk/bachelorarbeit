/**************************************************************************************************
 * Groovy-Skript:  SAP MDI → SAP CDC  –  B2B Contact Update
 *
 * Hinweis:
 *  • Alle Funktionen sind modular aufgebaut und deutsch kommentiert.
 *  • An jeder relevanten Stelle wird ein aussagekräftiges Error-Handling ausgeführt.
 *  • Bei auftretenden Fehlern wird der aktuelle Payload als Attachment dem MessageLog beigefügt.
 **************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.gateway.ip.core.customdev.util.MessageLog
import groovy.json.JsonOutput

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.net.HttpURLConnection

/* ================================================================================================
 * Haupt-Einstiegspunkt
 * ============================================================================================== */
Message processData(Message message) {

    // MessageLog instanziieren (kann ‘null’ sein, wenn Monitoring deaktiviert ist)
    MessageLog mLog = messageLogFactory?.getMessageLog(message)

    // Komplette Verarbeitung absichern
    try {

        /* 1. Header & Properties ermitteln ------------------------------------------------------ */
        Map cfg = determineConfiguration(message, mLog)

        /* 2. BusinessPartner-XML analysieren ---------------------------------------------------- */
        Map<String, String> incomingProfile = mapIncomingProfile(message, mLog)

        /* 3. JWT & Signatur erzeugen ------------------------------------------------------------ */
        Map jwtData = buildJwtAndSignature(cfg, mLog)

        /* 4. Account aus CDC lesen -------------------------------------------------------------- */
        Map<String, Object> cdcProfile = getAccount(cfg, jwtData, mLog)

        /* 5. Profil-Vergleich & Merge ----------------------------------------------------------- */
        Map<String, Object> mergedProfile = mergeProfiles(cdcProfile, incomingProfile, mLog)

        /* 6. Update-Aufruf an CDC --------------------------------------------------------------- */
        if (mergedProfile) {
            updateProfile(cfg, jwtData, mergedProfile, mLog)
        }

        // Wir geben den (unveränderten) Message-Body zurück
        return message

    } catch (Exception e) {
        handleError(message.getBody(String) as String, e, mLog)
        return message        // wird aufgrund von Exception nie erreicht, Compiler-Beruhigung
    }
}

/* ================================================================================================
 *  Funktion: Bestimmt benötigte Header & Properties
 * ============================================================================================== */
private Map determineConfiguration(Message msg, MessageLog mLog) {

    // Hilfsclosure – sucht zuerst in Properties, dann in Headern, sonst placeholder
    def val = { String key ->
        msg.getProperty(key) ?: msg.getHeader(key, String) ?: 'placeholder'
    }

    Map cfg = [
            requestUser              : val('requestUser'),
            requestPassword          : val('requestPassword'),
            requestURL               : val('requestURL'),
            apiKey                   : val('apiKey'),
            plainHMACKeyForSignature : val('plainHMACKeyForSignature'),
            profileID                : null                                     // wird später ermittelt
    ]

    // Grundlegende Plausibilitätsprüfung
    ['requestURL', 'apiKey', 'plainHMACKeyForSignature'].each { key ->
        if (cfg[key] == 'placeholder') {
            throw new IllegalStateException("Pflicht-Property '${key}' nicht gefunden.")
        }
    }

    mLog?.addAttachmentAsString('Configuration', JsonOutput.prettyPrint(JsonOutput.toJson(cfg)), 'application/json')
    return cfg
}

/* ================================================================================================
 *  Funktion: Liest BusinessPartner-XML ein und erstellt ein Profil-Objekt
 * ============================================================================================== */
private Map<String, String> mapIncomingProfile(Message msg, MessageLog mLog) {

    final String xmlPayload = msg.getBody(String) as String
    def root = new XmlSlurper(false, false).parseText(xmlPayload)

    // Namespace beachten (Default-NS)
    root = root.name() == 'BusinessPartner' ? root : root.'**'.find { it.name() == 'BusinessPartner' }
    if (!root) {
        throw new IllegalArgumentException('BusinessPartner Element im Payload nicht gefunden.')
    }

    // Profil-ID ermitteln und als Property für nachfolgende Schritte speichern
    String profileId = root.Common.IAMID.text()
    if (!profileId) {
        throw new IllegalArgumentException('IAMID (Profil-UID) ist leer.')
    }
    msg.setProperty('profileID', profileId)        // damit ggf. nachfolgende Steps darauf zugreifen
    mLog?.setStringProperty('profileID', profileId)

    Map<String, String> profile = [
            firstName : root.Name.FirstName.text(),
            lastName  : root.Name.LastName.text(),
            city      : root.Address.CityName.text()
            // Weitere Felder können hier ergänzt werden
    ].findAll { it.value }                          // leere Einträge verwerfen

    mLog?.addAttachmentAsString('IncomingProfile', JsonOutput.prettyPrint(JsonOutput.toJson(profile)), 'application/json')
    return profile
}

/* ================================================================================================
 *  Funktion: Erstellt JWT-Header, -Claim und Signatur
 * ============================================================================================== */
private Map buildJwtAndSignature(Map cfg, MessageLog mLog) {

    long now = (System.currentTimeMillis() / 1000L) as long

    String jwtHeader = Base64.getUrlEncoder().withoutPadding()
                      .encodeToString(JsonOutput.toJson([alg: 'RS256', typ: 'JWT', kid: cfg.apiKey])
                      .getBytes(StandardCharsets.UTF_8))

    String jwtClaim = Base64.getUrlEncoder().withoutPadding()
                     .encodeToString(JsonOutput.toJson([iat: now])
                     .getBytes(StandardCharsets.UTF_8))

    String jwtToken = "${jwtHeader}.${jwtClaim}"

    // Signatur via HmacSHA256
    Mac mac = Mac.getInstance('HmacSHA256')
    SecretKeySpec sk = new SecretKeySpec(cfg.plainHMACKeyForSignature.getBytes(StandardCharsets.UTF_8), 'HmacSHA256')
    mac.init(sk)
    String signature = Base64.getEncoder().encodeToString(mac.doFinal(jwtToken.getBytes(StandardCharsets.UTF_8)))

    Map jwtData = [token: jwtToken, signature: signature]
    mLog?.addAttachmentAsString('JWT_Header_Claim', jwtToken, 'text/plain')
    return jwtData
}

/* ================================================================================================
 *  Funktion: Holt Profil aus SAP Customer Data Cloud
 * ============================================================================================== */
private Map<String, Object> getAccount(Map cfg, Map jwt, MessageLog mLog) {

    String basicAuth = Base64.getEncoder()
                       .encodeToString("${cfg.requestUser}:${cfg.requestPassword}".getBytes(StandardCharsets.UTF_8))

    URL url = new URL("${cfg.requestURL}/${cfg.profileID ?: cfg.profileID}")
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.with {
        requestMethod     = 'GET'
        connectTimeout    = 30000
        readTimeout       = 30000
        setRequestProperty('Authorization', "Basic ${basicAuth}")
        setRequestProperty('JWT-Token', jwt.token)
        setRequestProperty('CDCSignature', jwt.signature)
    }

    int rc = conn.responseCode
    mLog?.setStringProperty('CDC_GET_RC', rc.toString())

    if (rc == 200) {
        String response = conn.inputStream.getText('UTF-8')
        mLog?.addAttachmentAsString('CDC_GetAccount_Response', response, 'application/json')
        def json = new groovy.json.JsonSlurper().parseText(response)
        return (json?.profile ?: [:]) as Map<String, Object>
    } else if (rc == 404) {
        // Profil existiert noch nicht
        mLog?.addAttachmentAsString('CDC_GetAccount_Response', "Profil ${cfg.profileID} nicht gefunden (404).", 'text/plain')
        return [:]
    } else {
        String err = conn.errorStream?.getText('UTF-8') ?: ''
        throw new RuntimeException("Fehler beim Aufruf GET Account (HTTP $rc). Antwort: $err")
    }
}

/* ================================================================================================
 *  Funktion: Vergleicht CDC-Profil mit eingehendem Profil
 *             – Liefert zusammengeführtes Profil oder wirft Fehler, falls identisch
 * ============================================================================================== */
private Map mergeProfiles(Map existing, Map incoming, MessageLog mLog) {

    if (existing && existing == incoming) {
        throw new IllegalStateException('Keine Änderungen am Profil vorhanden – Update nicht erforderlich.')
    }

    // Zusammenführen (incoming überschreibt existing)
    Map merged = [:]
    merged.putAll(existing ?: [:])
    merged.putAll(incoming ?: [:])

    mLog?.addAttachmentAsString('MergedProfile', JsonOutput.prettyPrint(JsonOutput.toJson(merged)), 'application/json')
    return merged
}

/* ================================================================================================
 *  Funktion: Führt das Update-POST gegen CDC aus
 * ============================================================================================== */
private void updateProfile(Map cfg, Map jwt, Map profile, MessageLog mLog) {

    String basicAuth = Base64.getEncoder()
                       .encodeToString("${cfg.requestUser}:${cfg.requestPassword}".getBytes(StandardCharsets.UTF_8))

    String query = "apiKey=${URLEncoder.encode(cfg.apiKey, 'UTF-8')}"            +
                   "&UID=${URLEncoder.encode(cfg.profileID, 'UTF-8')}"            +
                   "&profile=${URLEncoder.encode(JsonOutput.toJson(profile), 'UTF-8')}"

    URL url = new URL("${cfg.requestURL}/${cfg.profileID}")
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.with {
        requestMethod     = 'POST'
        doOutput          = true
        connectTimeout    = 30000
        readTimeout       = 30000
        setRequestProperty('Authorization',  "Basic ${basicAuth}")
        setRequestProperty('Content-Type',   'application/x-www-form-urlencoded')
        setRequestProperty('JWT-Token',      jwt.token)
        setRequestProperty('CDCSignature',   jwt.signature)
    }

    conn.outputStream.withWriter('UTF-8') { it << query }
    int rc = conn.responseCode
    mLog?.setStringProperty('CDC_POST_RC', rc.toString())

    if (rc in [200, 201, 202]) {
        mLog?.addAttachmentAsString('CDC_Update_Response', conn.inputStream.getText('UTF-8'), 'application/json')
    } else {
        String err = conn.errorStream?.getText('UTF-8') ?: ''
        throw new RuntimeException("Fehler beim Update-POST (HTTP $rc). Antwort: $err")
    }
}

/* ================================================================================================
 *  Gemeinsames Error-Handling – legt Payload als Attachment ab und wirft Laufzeitfehler
 * ============================================================================================== */
private void handleError(String body, Exception e, MessageLog mLog) {
    mLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    String msg = "Fehler im Contact-Update-Skript: ${e.message}"
    throw new RuntimeException(msg, e)
}