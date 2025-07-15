/****************************************************************************************
 *  SAP CPI – Groovy-Skript
 *  Integration: SAP Marketing Cloud  <->  CELUM
 *
 *  Aufgaben:
 *  1. Ermitteln der Header/Properties und Default-Befüllung
 *  2. Entscheidung und Aufruf der API-Operationen „Connect“ bzw. „Search“
 *  3. Mapping der „Search“-Response auf das Zielschema
 *  4. Einheitliches Error-Handling inkl. Payload-Attachment
 *
 *  Autor:  (Senior Integration Developer)
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import java.net.HttpURLConnection

/* ------------------------------------------------------------------------
 *  Haupteinstieg
 * --------------------------------------------------------------------- */
Message processData(Message message) {

    // Zugriff auf das Message-Log für Monitoring-Einträge
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        // 1) Header / Properties auslesen bzw. mit Default „placeholder“ befüllen
        def cfg = resolveConfiguration(message, messageLog)

        // 2) Entscheidung, welche REST-Operation aufzurufen ist
        if (!cfg.keywordQuery || cfg.keywordQuery == 'placeholder') {
            // ---- CONNECT -----------------------------------------------------
            String result = performConnect(cfg, messageLog)
            message.setBody(result)                                     // Body = „connected“
        } else {
            // ---- SEARCH ------------------------------------------------------
            String responseJson = performSearch(cfg, messageLog)        // roher JSON-String
            String mappedJson   = mapSearchResponse(responseJson, messageLog)
            message.setBody(mappedJson)                                 // Body = gemapptes JSON
        }

        return message

    } catch (Exception e) {
        // Zentrales Error-Handling
        handleError(message.getBody(String) as String, e, messageLog)
        // handleError wirft Exception; return nur aus Syntax-Gründen
        return message
    }
}

/* =========================================================================
 *  Hilfs- und Service-Funktionen
 * ========================================================================= */

/* resolveConfiguration
 * Lies Header / Properties aus dem Message-Objekt oder setze „placeholder“.
 */
private Map resolveConfiguration(Message msg, def log) {

    Map cfg = [:]

    // --- Header ----------------------------------------------------------
    cfg.keywordQuery = getValue(msg.getHeaders(), 'keyword_query')

    // --- Properties ------------------------------------------------------
    cfg.damUser     = getValue(msg.getProperties(), 'DAMUser')
    cfg.damPassword = getValue(msg.getProperties(), 'DAMPassword')
    cfg.damUrl      = getValue(msg.getProperties(), 'DAMURL')

    log?.addAttachmentAsString('Config', cfg.toString(), 'text/plain')
    return cfg
}

/* getValue
 * Liefert Wert aus Map oder 'placeholder' wenn leer/nicht vorhanden.
 */
private String getValue(Map source, String key) {
    def val = source?.get(key) as String
    return (val && val.trim()) ? val.trim() : 'placeholder'
}

/* performConnect
 * Führt die REST-Operation „Connect“ aus.
 */
private String performConnect(Map cfg, def log) {

    HttpURLConnection conn = openConnection(cfg, null)
    int rc = conn.responseCode

    if (rc != 200) {
        throw new RuntimeException("Connect-Aufruf fehlgeschlagen. HTTP-Status: ${rc}")
    }

    log?.addAttachmentAsString('Connect-ResponseCode', rc.toString(), 'text/plain')
    return 'connected'
}

/* performSearch
 * Führt die REST-Operation „Search“ aus und liefert die Response als String.
 */
private String performSearch(Map cfg, def log) {

    HttpURLConnection conn = openConnection(cfg, cfg.keywordQuery)
    int rc = conn.responseCode

    if (rc != 200) {
        throw new RuntimeException("Search-Aufruf fehlgeschlagen. HTTP-Status: ${rc}")
    }

    String responseJson = conn.inputStream.getText('UTF-8')
    log?.addAttachmentAsString('Search-RawResponse', responseJson, 'application/json')
    return responseJson
}

/* openConnection
 * Erzeugt & konfiguriert HttpURLConnection inklusive Basic-Auth.
 */
private HttpURLConnection openConnection(Map cfg, String keywordQueryHeader) {

    URL url = new URL(cfg.damUrl)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.requestMethod = 'GET'

    // Basic Authentication Header
    String userPass   = "${cfg.damUser}:${cfg.damPassword}"
    String basicToken = 'Basic ' + userPass.bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', basicToken)

    // Such-Header nur wenn vorhanden
    if (keywordQueryHeader && keywordQueryHeader != 'placeholder') {
        conn.setRequestProperty('keyword_query', keywordQueryHeader)
    }

    conn.connect()
    return conn
}

/* mapSearchResponse
 * Wandelt die Search-JSON-Antwort ins geforderte Ziel-JSON.
 */
private String mapSearchResponse(String sourceJson, def log) {

    // --- JSON einlesen ---------------------------------------------------
    def json     = new JsonSlurper().parseText(sourceJson)
    def assetsIn = json?.assetsList?.assets ?: []

    // --- Validierung rudimentär -----------------------------------------
    if (!assetsIn) {
        throw new RuntimeException('Search-Response enthält keine Assets.')
    }

    // --- Mapping ---------------------------------------------------------
    List assetsOut = assetsIn.collect { wrapper ->

        def asset   = wrapper.asset
        def assetId = asset.assetId

        // Varianten -> DIGITALASSETFILE-Array
        def files = asset.variants.collect { v ->
            [
                ASSET_ID : assetId,
                FILE_ID  : v.fileId,
                URL      : v.url
            ]
        }

        // Ergebnis-Objekt
        [
            ASSET: [
                ASSET_ID        : assetId,
                KEYWORDS        : asset.keywords,
                DESCRIPTION     : asset.description,
                DIGITALASSETFILE: files
            ]
        ]
    }

    // --- Ziel-Struktur ---------------------------------------------------
    def targetJson = [Assets: assetsOut]

    String pretty = new JsonBuilder(targetJson).toPrettyString()
    log?.addAttachmentAsString('Mapped-Response', pretty, 'application/json')
    return pretty
}

/* handleError
 * Zentrales Error-Handling. Fügt den fehlerhaften Payload als Attachment an
 * und wirft anschließend eine RuntimeException, damit CPI die Exception
 * korrekt propagated.
 */
private void handleError(String body, Exception e, def messageLog) {
    // Logging im Monitoring
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '<empty>', 'text/plain')
    def errorMsg = "Fehler im CPI-Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}