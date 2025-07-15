/****************************************************************************************
 *  Groovy-Script – SAP Cloud Integration
 *  Aufgabe:      SAP Marketing Cloud  <->  CELUM  –  Assets abrufen
 *  Autor:        ChatGPT (Senior-Developer)
 *  Datum:        2025-06-16
 *
 *  Dieses Skript
 *    1. liest benötigte Header & Properties aus dem Message-Objekt,
 *    2. entscheidet, ob ein „Connect-“ oder „Search-“-Request ausgeführt wird,
 *    3. führt den HTTP-Aufruf mit Basic-Authentication aus,
 *    4. mappt die JSON-Antwort auf das gewünschte Zielschema,
 *    5. liefert Ergebnis oder wirft bei einem Fehler eine aussagekräftige Exception.
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.net.HttpURLConnection
import java.net.URL

/*----------------------------------------------------------
  Haupteinstieg
----------------------------------------------------------*/
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    String inboundBody = message.getBody(String) ?: ''

    try {
        // 1. Konfiguration ermitteln
        Map cfg = loadConfiguration(message)

        // 2. Entscheidung: Connect oder Search
        String responseBody
        if (!cfg.keywordQuery || cfg.keywordQuery == 'placeholder') {
            responseBody = executeConnect(cfg, messageLog)
        } else {
            responseBody = executeSearch(cfg, messageLog)
            // 3. Mapping der Response
            responseBody = mapResponse(responseBody)
        }

        message.setBody(responseBody)
        return message
    } catch (Exception e) {
        // zentrales Error-Handling
        handleError(inboundBody, e, messageLog)
        return message   // wird niemals erreicht, handleError wirft Exception
    }
}

/*----------------------------------------------------------
  Konfiguration ermitteln
----------------------------------------------------------*/
private Map loadConfiguration(Message message) {
    // Header & Properties lesen, fehlende Werte mit "placeholder" ersetzen
    Map cfg = [:]

    cfg.keywordQuery = (message.getHeader('keyword_query', String) ?: '').trim()
    if (!cfg.keywordQuery) { cfg.keywordQuery = 'placeholder' }

    cfg.user  = (message.getProperty('DAMUser' ) ?: '').toString()
    cfg.pass  = (message.getProperty('DAMPassword') ?: '').toString()
    cfg.url   = (message.getProperty('DAMURL') ?: '').toString()

    if (!cfg.user ) cfg.user  = 'placeholder'
    if (!cfg.pass ) cfg.pass  = 'placeholder'
    if (!cfg.url  ) cfg.url   = 'placeholder'

    return cfg
}

/*----------------------------------------------------------
  HTTP-Request:  CONNECT
----------------------------------------------------------*/
private String executeConnect(Map cfg, def messageLog) {
    HttpURLConnection conn
    try {
        conn = (HttpURLConnection) new URL(cfg.url).openConnection()
        conn.with {
            requestMethod = 'GET'
            setRequestProperty('Authorization', buildBasicAuth(cfg.user, cfg.pass))
            connectTimeout = 15000
            readTimeout    = 15000
        }

        int rc = conn.responseCode
        messageLog?.addAttachmentAsString('Connect-Status', rc.toString(), 'text/plain')

        if (rc != 200) {
            throw new RuntimeException("Connect-Aufruf nicht erfolgreich. HTTP-Status: $rc")
        }
        return 'connected'
    } finally {
        conn?.disconnect()
    }
}

/*----------------------------------------------------------
  HTTP-Request:  SEARCH
----------------------------------------------------------*/
private String executeSearch(Map cfg, def messageLog) {
    HttpURLConnection conn
    try {
        conn = (HttpURLConnection) new URL(cfg.url).openConnection()
        conn.with {
            requestMethod = 'GET'
            setRequestProperty('Authorization', buildBasicAuth(cfg.user, cfg.pass))
            setRequestProperty('keyword_query', cfg.keywordQuery)
            setRequestProperty('Accept', 'application/json')
            connectTimeout = 20000
            readTimeout    = 20000
        }

        int rc = conn.responseCode
        String response = rc == 200 ? conn.inputStream.getText('UTF-8')
                                    : conn.errorStream?.getText('UTF-8')

        messageLog?.addAttachmentAsString('Search-Response-RAW', response, 'application/json')

        if (rc != 200) {
            throw new RuntimeException("Search-Aufruf fehlgeschlagen. HTTP-Status: $rc – Response: $response")
        }
        return response
    } finally {
        conn?.disconnect()
    }
}

/*----------------------------------------------------------
  RESPONSE-MAPPING  (Source  ->  Target)
----------------------------------------------------------*/
private String mapResponse(String jsonString) {
    def slurper = new JsonSlurper()
    def source  = slurper.parseText(jsonString ?: '{}')

    if (!source?.assetsList?.assets) {
        throw new RuntimeException('Pfad assetsList.assets nicht vorhanden oder leer – Mapping abgebrochen.')
    }

    List assetsOut = []

    source.assetsList.assets.each { assetWrapper ->
        def asset = assetWrapper?.asset
        // Validierung der Pflichtfelder
        assertField(asset?.assetId,      'assetId')
        assertField(asset?.description,  'description')
        assertField(asset?.variants,     'variants')
        assertField(asset?.keywords,     'keywords')

        Map<String, Object> assetOut = [
                ASSET_ID   : asset.assetId as String,
                KEYWORDS   : asset.keywords as String,
                DESCRIPTION: asset.description as String,
                DIGITALASSETFILE: []
        ]

        asset.variants.each { variant ->
            assertField(variant?.fileId, 'fileId (variant)')
            assertField(variant?.url,    'url (variant)')

            assetOut.DIGITALASSETFILE << [
                    ASSET_ID: asset.assetId as String,
                    FILE_ID : variant.fileId as String,
                    URL     : variant.url as String
            ]
        }

        assetsOut << [ASSET: assetOut]
    }

    Map target = [Assets: assetsOut]
    return JsonOutput.prettyPrint(JsonOutput.toJson(target))
}

/*----------------------------------------------------------
  Hilfsfunktionen
----------------------------------------------------------*/
private static void assertField(def field, String fieldName) {
    if (field == null || (field instanceof CharSequence && field.toString().trim().isEmpty())) {
        throw new RuntimeException("Pflichtfeld '$fieldName' fehlt oder ist leer.")
    }
}

private static String buildBasicAuth(String user, String pass) {
    return 'Basic ' + "${user}:${pass}".bytes.encodeBase64().toString()
}

/*----------------------------------------------------------
  Zentrales Error-Handling
----------------------------------------------------------*/
private void handleError(String body, Exception e, def messageLog) {
    // Ursprüngliche Payload als Attachment anhängen
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}