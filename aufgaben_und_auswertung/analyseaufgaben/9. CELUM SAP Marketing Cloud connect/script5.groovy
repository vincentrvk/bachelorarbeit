/****************************************************************************************
*  SAP Cloud Integration – Groovy Skript
*  Aufgabe:   Anbindung CELUM DAM – Connect / Search & Response-Mapping
*  Autor:     ChatGPT (Senior-Entwickler Integrationen)
*****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import java.nio.charset.StandardCharsets

/* ================================================================
 *  Einstiegspunkt
 * ================================================================ */
Message processData(Message message) {
    // MessageLog für Monitoring
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        // 1. Konfigurationswerte (Header & Properties) lesen
        def cfg = readConfiguration(message)

        // 2. Entscheidung ob „Connect“ oder „Search“
        def responsePayload
        if (!cfg.keywordQuery || cfg.keywordQuery == 'placeholder') {
            responsePayload = callConnect(cfg, messageLog)
        } else {
            def rawResponse = callSearch(cfg, messageLog)
            responsePayload  = mapResponse(rawResponse, messageLog)
        }

        // 3. Body setzen und zurückgeben
        message.setBody(responsePayload)
        return message

    } catch (Exception e) {
        // Zentrales Error-Handling
        handleError(message.getBody(String) as String ?: '', e, messageLog)
        return message  // wird nie erreicht – handleError wirft Exception
    }
}

/* ================================================================
 *  Konfigurationswerte bestimmen
 * ================================================================ */
private Map readConfiguration(Message message) {
    /*
     * Liest Header / Properties falls vorhanden,
     * sonst Default „placeholder“
     */
    def header      = { name   -> (message.getHeader(name,    String) ?: 'placeholder') }
    def propertyVal = { name   -> (message.getProperty(name)  ?: 'placeholder')        }

    return [
        keywordQuery : header('keyword_query'),
        damUrl       : propertyVal('DAMURL'),
        damUser      : propertyVal('DAMUser'),
        damPassword  : propertyVal('DAMPassword')
    ]
}

/* ================================================================
 *  API-Call: Connect
 * ================================================================ */
private String callConnect(Map cfg, def messageLog) {
    /*
     * Führt einen GET ohne Header durch, um zu prüfen,
     * ob eine Verbindung aufgebaut werden kann.
     * Liefert bei HTTP-Status 200 das Wort „connected“ zurück.
     */
    def result = httpGet(cfg.damUrl, cfg, [:], messageLog)
    return 'connected'
}

/* ================================================================
 *  API-Call: Search
 * ================================================================ */
private String callSearch(Map cfg, def messageLog) {
    /*
     * Führt einen GET mit Header „keyword_query“ durch
     * und liefert den Response-Body als String zurück.
     */
    def headers = ['keyword_query': cfg.keywordQuery]
    return httpGet(cfg.damUrl, cfg, headers, messageLog)
}

/* ================================================================
 *  Generische HTTP-GET-Funktion
 * ================================================================ */
private String httpGet(String urlStr, Map cfg, Map<String,String> headers, def messageLog) {
    try {
        def url        = new URL(urlStr)
        def connection = url.openConnection()
        connection.setRequestMethod('GET')

        // Basic-Auth setzen
        def auth = "${cfg.damUser}:${cfg.damPassword}"
        def enc  = auth.getBytes(StandardCharsets.UTF_8).encodeBase64().toString()
        connection.setRequestProperty('Authorization', "Basic $enc")

        // Zusatz-Header
        headers.each { k, v -> connection.setRequestProperty(k, v) }

        // Request ausführen
        def rc = connection.responseCode
        messageLog.addAttachmentAsString('HTTP-Status', rc.toString(), 'text/plain')

        if (rc != 200) {
            throw new RuntimeException("Unerwarteter HTTP-Status: $rc")
        }

        // Response lesen
        return connection.inputStream.getText(StandardCharsets.UTF_8.name())

    } catch (Exception e) {
        throw new RuntimeException("HTTP-GET fehlgeschlagen: ${e.message}", e)
    }
}

/* ================================================================
 *  Mapping der Search-Antwort auf Zielschema
 * ================================================================ */
private String mapResponse(String rawJson, def messageLog) {
    /*
     * Erwartete Eingangsstruktur siehe Aufgabenstellung.
     * Liefert einen formatierten JSON-String gemäß Output-Schema.
     */
    try {
        def json       = new JsonSlurper().parseText(rawJson)
        def assetsIn   = json?.assetsList?.assets ?: []

        def assetsOut = assetsIn.collect { wrapper ->
            def asset     = wrapper?.asset
            def assetId   = asset?.assetId
            def desc      = asset?.description
            def keywords  = asset?.keywords
            def variants  = asset?.variants ?: []

            [
                'ASSET': [
                    'ASSET_ID'        : assetId,
                    'KEYWORDS'        : keywords,
                    'DESCRIPTION'     : desc,
                    'DIGITALASSETFILE': variants.collect { v ->
                        [
                            'ASSET_ID': assetId,
                            'FILE_ID' : v?.fileId,
                            'URL'     : v?.url
                        ]
                    }
                ]
            ]
        }

        def resultMap = ['Assets': assetsOut]

        def prettyJson = new JsonBuilder(resultMap).toPrettyString()
        messageLog.addAttachmentAsString('MappedPayload', prettyJson, 'application/json')
        return prettyJson

    } catch (Exception e) {
        throw new RuntimeException("Fehler im Response-Mapping: ${e.message}", e)
    }
}

/* ================================================================
 *  Zentrales Error-Handling
 * ================================================================ */
private void handleError(String body, Exception e, def messageLog) {
    /*
     * Anhängen des fehlerhaften Payloads und Werfen einer Runtime-Exception
     * zur Abbruch-Kennzeichnung in SAP CPI.
     */
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/plain')
    def errorMsg = "Groovy-Skript Fehler: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}