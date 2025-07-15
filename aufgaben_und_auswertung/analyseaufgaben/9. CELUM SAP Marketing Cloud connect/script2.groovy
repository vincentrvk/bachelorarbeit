/*************************************************************************************************
 * Groovy-Skript:  SAP Marketing Cloud <-> CELUM  – Asset Information
 *
 * Autor:        (automatisch generiert)
 * Beschreibung: Siehe Aufgabenstellung
 *************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets

/* ==============================================================================================
 *  Haupteinstieg
 * ============================================================================================ */
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {

        /* 1) Header/Property-Werte setzen bzw. Platzhalter vergeben */
        def ctx = setContextValues(message)

        /* 2) Entscheidung Connect/Search */
        if (ctx.keyword_query == 'placeholder' || ctx.keyword_query.trim().isEmpty()) {

            /* --- CONNECT --------------------------------------------------------------- */
            def connectResult = callConnect(ctx, messageLog)
            message.setBody(connectResult)
            message.setHeader('Content-Type', 'text/plain')

        } else {

            /* --- SEARCH ---------------------------------------------------------------- */
            def searchResponse = callSearch(ctx, messageLog)
            def mapped       = mapResponse(searchResponse, messageLog)

            message.setBody(mapped)
            message.setHeader('Content-Type', 'application/json')
        }

        return message

    } catch (Exception e) {
        /* Zentrales Error-Handling – wirft RuntimeException weiter */
        handleError(message.getBody(String) ?: '', e, messageLog)
        return message  // wird nie erreicht, aber vom Compiler verlangt
    }
}

/* ==============================================================================================
 *  Funktionsblock – Kontext (Headers & Properties)
 * ============================================================================================ */
/**
 * Liest benötigte Header & Properties, vergibt ggf. "placeholder" und
 * speichert die ermittelten Werte in einer Map.
 */
private static Map setContextValues(Message message) {

    def keyword_query = message.getHeader('keyword_query', String) ?: 'placeholder'

    def damUser       = message.getProperty('DAMUser' ) ?: 'placeholder'
    def damPassword   = message.getProperty('DAMPassword') ?: 'placeholder'
    def damUrl        = message.getProperty('DAMURL') ?: 'placeholder'

    /* Für nachfolgende Schritte Header/Properties sicherstellen                */
    message.setHeader   ('keyword_query', keyword_query)
    message.setProperty ('DAMUser',       damUser)
    message.setProperty ('DAMPassword',   damPassword)
    message.setProperty ('DAMURL',        damUrl)

    return [
            keyword_query : keyword_query,
            user          : damUser,
            password      : damPassword,
            url           : damUrl
    ]
}

/* ==============================================================================================
 *  Funktionsblock – API Calls
 * ============================================================================================ */
/**
 * Führt einen "Connect" – Aufruf durch und gibt bei Erfolg den String "connected" zurück.
 */
private static String callConnect(Map ctx, def messageLog) {

    HttpURLConnection con = null
    try {
        con = openConnection(ctx.url, ctx.user, ctx.password)
        int rc = con.responseCode

        messageLog?.addAttachmentAsString('ConnectStatus', "HTTP ${rc}", 'text/plain')

        if (rc in 200..299) {
            return 'connected'
        }
        throw new RuntimeException("Connect-Aufruf fehlgeschlagen – HTTP Code: ${rc}")

    } finally {
        con?.disconnect()
    }
}

/**
 * Führt einen "Search" – Aufruf durch und liefert die Response (JSON-String) zurück.
 */
private static String callSearch(Map ctx, def messageLog) {

    HttpURLConnection con = null
    try {
        con = openConnection(ctx.url, ctx.user, ctx.password)
        con.setRequestProperty('keyword_query', ctx.keyword_query)

        int rc = con.responseCode
        def response = con.inputStream.getText(StandardCharsets.UTF_8.name())

        messageLog?.addAttachmentAsString('SearchStatus', "HTTP ${rc}", 'text/plain')
        messageLog?.addAttachmentAsString('SearchResponseRaw', response, 'application/json')

        if (rc in 200..299) {
            return response
        }
        throw new RuntimeException("Search-Aufruf fehlgeschlagen – HTTP Code: ${rc}")

    } finally {
        con?.disconnect()
    }
}

/* ==============================================================================================
 *  Funktionsblock – Mapping
 * ============================================================================================ */
/**
 * Wandelt die CELUM – Antwort gemäß Zielschema in Ziel-JSON um.
 */
private static String mapResponse(String jsonString, def messageLog) {

    def slurper = new JsonSlurper()
    def src     = slurper.parseText(jsonString)

    def result = [
            Assets: src?.assetsList?.assets.collect { assetWrapper ->

                def a    = assetWrapper?.asset
                def files = a?.variants?.collect { v ->
                    [
                            ASSET_ID: a?.assetId ?: '',
                            FILE_ID : v?.fileId ?: '',
                            URL     : v?.url ?: ''
                    ]
                }

                return [
                        ASSET: [
                                ASSET_ID        : a?.assetId ?: '',
                                KEYWORDS        : a?.keywords ?: '',
                                DESCRIPTION     : a?.description ?: '',
                                DIGITALASSETFILE: files ?: []
                        ]
                ]
            } ?: []
    ]

    def jsonOut = new JsonBuilder(result).toPrettyString()
    messageLog?.addAttachmentAsString('MappedResponse', jsonOut, 'application/json')
    return jsonOut
}

/* ==============================================================================================
 *  Helferfunktionen
 * ============================================================================================ */
/** Erstellt und konfiguriert eine HTTP-GET Verbindung inkl. Basic-Auth */
private static HttpURLConnection openConnection(String url, String user, String pass) {
    def connection = (HttpURLConnection) new URL(url).openConnection()
    connection.requestMethod = 'GET'
    connection.setDoOutput(false)

    String auth = "${user}:${pass}".bytes.encodeBase64().toString()
    connection.setRequestProperty('Authorization', "Basic ${auth}")
    connection.setConnectTimeout(15000)
    connection.setReadTimeout   (30000)
    return connection
}

/* ==============================================================================================
 *  Error-Handling
 * ============================================================================================ */
/**
 * Zentraler Error-Handler: legt den Payload als Attachment ab und wirft RuntimeException weiter.
 */
private static void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/plain')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}