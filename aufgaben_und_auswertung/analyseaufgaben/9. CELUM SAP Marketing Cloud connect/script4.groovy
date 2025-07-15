/****************************************************************************
 *  Groovy-Skript:  CELUM – SAP Marketing Cloud Integration
 *  Zweck        :  Führt je nach übergebenem Header entweder einen
 *                  „Connect-“ oder „Search-“ Aufruf gegen CELUM aus
 *                  und liefert – im Falle eines Suchaufrufes – die
 *                  gemappte Antwortstruktur zurück.
 *  Autor        :  ChatGPT  –  Senior Integration Developer
 ***************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

// ========================================================================
//  Haupt-Entry-Point
// ========================================================================
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    def requestBody = message.getBody(String) ?: ""

    try {
        // 1) Konfiguration ermitteln (Header & Properties)
        def cfg = extractConfig(message, messageLog)

        // 2) Entscheidung, welche Anfrage ausgeführt werden muss
        if (!cfg.keywordQuery || cfg.keywordQuery == "placeholder") {
            // --> CONNECT
            String resp = executeConnect(cfg, messageLog)
            message.setBody(resp)
        } else {
            // --> SEARCH
            String searchRaw = executeSearch(cfg, messageLog)
            String mapped   = mapResponse(searchRaw, messageLog)
            message.setBody(mapped)
            message.setHeader("Content-Type", "application/json")
        }

    } catch (Exception e) {
        handleError(requestBody, e, messageLog)
    }

    return message
}

// ========================================================================
//  Modul 1 – Header & Property-Handling
// ========================================================================
/**
 * Liest Header & Properties aus, legt Default-Werte (placeholder) und
 * liefert alle relevanten Parameter als Map zurück.
 */
def extractConfig(Message msg, def log) {
    Map<String, Object> cfg = [:]

    // Header
    cfg.keywordQuery = msg.getHeader("keyword_query", String.class) ?: "placeholder"

    // Properties
    cfg.damUser     = msg.getProperty("DAMUser")     ?: "placeholder"
    cfg.damPassword = msg.getProperty("DAMPassword") ?: "placeholder"
    cfg.damUrl      = msg.getProperty("DAMURL")      ?: "placeholder"

    // Für spätere Schritte als Header setzen, damit andere IFlow-Schritte
    // diese Info ggf. übernehmen können.
    msg.setHeader("keyword_query", cfg.keywordQuery)

    log?.addAttachmentAsString("Debug-Config", cfg.toString(), "text/plain")
    return cfg
}

// ========================================================================
//  Modul 2 – API-Aufrufe
// ========================================================================
/**
 * Führt den CONNECT-Aufruf gegen CELUM aus.
 * Gibt bei erfolgreicher Verbindung den String "connected" zurück.
 */
def executeConnect(Map cfg, def log) {
    HttpURLConnection conn = null
    try {
        conn = (HttpURLConnection) new URL(cfg.damUrl).openConnection()
        conn.requestMethod = "GET"
        conn.setRequestProperty("Authorization", buildBasicAuth(cfg))
        conn.connect()

        int rc = conn.responseCode
        log?.addAttachmentAsString("Connect-Status", "HTTP ${rc}", "text/plain")

        if (rc == 200) {
            return "connected"
        } else {
            throw new RuntimeException("CONNECT-Aufruf fehlgeschlagen – HTTP ${rc}")
        }
    } finally {
        conn?.disconnect()
    }
}

/**
 * Führt den SEARCH-Aufruf aus und liefert die rohe JSON-Antwort als String.
 */
def executeSearch(Map cfg, def log) {
    HttpURLConnection conn = null
    try {
        conn = (HttpURLConnection) new URL(cfg.damUrl).openConnection()
        conn.requestMethod = "GET"
        conn.setRequestProperty("Authorization", buildBasicAuth(cfg))
        conn.setRequestProperty("keyword_query", cfg.keywordQuery)
        conn.connect()

        int rc = conn.responseCode
        if (rc != 200) {
            throw new RuntimeException("SEARCH-Aufruf fehlgeschlagen – HTTP ${rc}")
        }

        String raw = conn.inputStream.getText(StandardCharsets.UTF_8.name())
        log?.addAttachmentAsString("Raw-Search-Response", raw, "application/json")
        return raw
    } finally {
        conn?.disconnect()
    }
}

/**
 * Erzeugt den Basic-Auth Header-Wert.
 */
private String buildBasicAuth(Map cfg) {
    return "Basic " + "${cfg.damUser}:${cfg.damPassword}".bytes.encodeBase64().toString()
}

// ========================================================================
//  Modul 3 – Mapping
// ========================================================================
/**
 * Mappt die CELUM-Antwort (Input Schema) auf das benötigte Zielschema.
 * Liefert ein prettified JSON-String des Zielschemas.
 */
def mapResponse(String inputJson, def log) {
    def slurper = new JsonSlurper()
    def parsed  = slurper.parseText(inputJson)

    def out = [Assets: []]

    parsed?.assetsList?.assets?.each { assetWrapper ->
        def asset = assetWrapper.asset
        def mappedAsset = [
            ASSET_ID    : asset.assetId,
            KEYWORDS    : asset.keywords ?: "",
            DESCRIPTION : asset.description ?: "",
            DIGITALASSETFILE: asset.variants.collect { v ->
                [
                    ASSET_ID: asset.assetId,
                    FILE_ID : v.fileId,
                    URL     : v.url
                ]
            }
        ]
        out.Assets << [ASSET: mappedAsset]
    }

    String result = JsonOutput.prettyPrint(JsonOutput.toJson(out))
    log?.addAttachmentAsString("Mapped-Response", result, "application/json")
    return result
}

// ========================================================================
//  Modul 4 – Zentrales Error-Handling
// ========================================================================
/**
 * Globale Fehlerbehandlung.
 * Fügt den eingehenden Payload als Attachment hinzu und wirft eine
 * RuntimeException mit einer eindeutigen Fehlermeldung.
 */
def handleError(String body, Exception e, def messageLog) {
    // Logging im Monitoring (name, inhalt, typ)
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/plain")
    String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}