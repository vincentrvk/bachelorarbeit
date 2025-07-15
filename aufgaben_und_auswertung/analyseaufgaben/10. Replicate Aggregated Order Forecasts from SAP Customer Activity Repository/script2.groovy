/************************************************************************************************
 * Groovy-Skript: Replicate Forecasts – SAP Cloud Integration
 * Autor:        ChatGPT (Senior Integration Entwickler)
 * Beschreibung:
 *   – Ermittelt benötigte Properties/Header aus dem Message-Objekt (oder Default „placeholder“)
 *   – Optionales Logging des ursprünglichen Payloads als Attachment
 *   – Optionales Mapping gem. Vorgaben (→ erzeugt Request-JSON)
 *   – Aufruf der Ziel-API via HTTP-POST (Basic-Auth)
 *   – Umfassendes Error-Handling mit Payload-Anlage
 * Modularer Aufbau entsprechend der Anforderungen.
 ************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.TimeZone
import java.net.HttpURLConnection
import java.nio.charset.StandardCharsets

/* ===================================================== *
 *  H A U P T P R O Z E S S
 * ===================================================== */
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        /* 1. Kontextwerte lesen bzw. vorbelegen */
        def ctx = readContext(message)

        /* 2. Optionales Logging des Eingangs-Payloads */
        if (ctx.enableLogging) {
            logPayload(messageLog, "InboundPayload", ctx.originalBody)
        }

        /* 3. Optionales Mapping durchführen */
        def requestPayload = ctx.originalBody
        if (ctx.enableMapping) {
            requestPayload = mapRequest(ctx.originalBody, ctx.forecastCodeMap)
        }

        /* 4. API-Call ausführen */
        invokeReplicateForecastAPI(ctx, requestPayload, messageLog)

        /* 5. Request-Payload als Body für Folge-Schritt bereitstellen */
        message.setBody(requestPayload)
        return message
    } catch (Exception e) {
        handleError(message.getBody(String) as String, e, messageLog)
        /* handleError wirft RuntimeException – nachfolgender Code wird nicht erreicht   */
    }
}

/* ===================================================== *
 *  F U N K T I O N E N
 * ===================================================== */

/* ----------------------------------------------------------------------------
 * Liest Properties / Header aus dem Message-Objekt oder vergibt Defaultwerte.
 * ---------------------------------------------------------------------------- */
private Map readContext(Message message) {
    Map ctx = [:]
    ctx.originalBody   = message.getBody(String) ?: ''
    ctx.requestUser    = message.getProperty('requestUser')    ?: 'placeholder'
    ctx.requestPass    = message.getProperty('requestPassword')?: 'placeholder'
    ctx.requestURL     = message.getProperty('requestURL')     ?: 'placeholder'
    ctx.enableLogging  = (message.getProperty('enableLogging') ?: 'false').toString().toBoolean()
    ctx.enableMapping  = (message.getProperty('enableMapping') ?: 'false').toString().toBoolean()

    /* Forecast-Code-Mapping laden (kann Map oder JSON-String sein) */
    def fcMapProp = message.getProperty('forecastCodeMap')
    if (fcMapProp instanceof Map) {
        ctx.forecastCodeMap = fcMapProp?.forecastCodeMap ?: [:]
    } else if (fcMapProp instanceof String && fcMapProp.trim()) {
        ctx.forecastCodeMap = new JsonSlurper().parseText(fcMapProp)?.forecastCodeMap ?: [:]
    } else {
        ctx.forecastCodeMap = [:]
    }
    return ctx
}

/* ----------------------------------------------------------------------------
 * Führt das Request-Mapping gem. Vorgaben durch.
 * ---------------------------------------------------------------------------- */
private String mapRequest(String body, Map codeMap) {
    def slurper = new JsonSlurper()
    def source  = slurper.parseText(body)

    /* -------- entityVersion -------- */
    def versionParts = (source.entityVersion ?: '').tokenize('.')
    if (versionParts.size() != 3) {
        throw new IllegalArgumentException("entityVersion hat kein gültiges SemVer-Format")
    }
    def entityVersionObj = [
        major: versionParts[0] as Integer,
        minor: versionParts[1] as Integer,
        patch: versionParts[2] as Integer
    ]

    /* -------- value-Liste transformieren -------- */
    def valueList = source.value.collect { item ->
        /* Produkt-ID anpassen */
        def prodId = item.product.id?.replaceFirst(/^PROD-/, 'PRODUCT-')

        /* Werk anreichern */
        def plantId = 'PLANT-' + item.supplyingPlant.id

        /* Forecast-Code mappen */
        def fc = codeMap?.get(item.forecastCode) ?: item.forecastCode

        /* Datum konvertieren: ISO8601 -> dd.MM.yyyy HH:mm */
        def aoq = item.aggregatedOrderQuantities.collect { q ->
            def isoDate  = Instant.parse(q.productStagingStart)
            def sdf      = new SimpleDateFormat("dd.MM.yyyy HH:mm")
            sdf.timeZone = TimeZone.getTimeZone('UTC')
            [productStagingStart: sdf.format(Date.from(isoDate))]
        }

        /* UNIX-Epoch -> ISO8601 (UTC) */
        def tsIso = Instant.ofEpochSecond(item.versionTimestamp as Long).toString()

        [
            product:        [id: prodId],
            supplyingPlant: [id: plantId],
            forecastCode:   fc,
            aggregatedOrderQuantities: aoq,
            versionTimestamp: tsIso
        ]
    }

    /* -------- Ziel-Struktur aufbauen & zurückgeben -------- */
    def target = [
        entityVersion: entityVersionObj,
        value        : valueList
    ]
    return new JsonBuilder(target).toString()
}

/* ----------------------------------------------------------------------------
 * Führt den tatsächlichen HTTP-Aufruf aus.
 * ---------------------------------------------------------------------------- */
private void invokeReplicateForecastAPI(Map ctx, String payload, def messageLog) {
    HttpURLConnection conn = null
    try {
        conn = (HttpURLConnection) new URL(ctx.requestURL).openConnection()
        conn.setRequestMethod("POST")
        String auth = "${ctx.requestUser}:${ctx.requestPass}"
        String encAuth = auth.bytes.encodeBase64().toString()
        conn.setRequestProperty("Authorization", "Basic ${encAuth}")
        conn.setRequestProperty("Content-Type", "application/json;charset=UTF-8")
        conn.setDoOutput(true)

        conn.outputStream.withWriter("UTF-8") { it << payload }

        int rc = conn.responseCode
        messageLog?.addAttachmentAsString("HTTP-Status", rc.toString(), "text/plain")

        if (rc < 200 || rc >= 300) {
            def err = conn.errorStream?.getText('UTF-8')
            throw new RuntimeException("HTTP-Fehler ${rc}: ${err ?: 'keine Fehlermeldung'}")
        }
        /* Response wird laut Vorgabe nicht weiterverarbeitet                     */
    } finally {
        conn?.disconnect()
    }
}

/* ----------------------------------------------------------------------------
 * Fügt ein Attachment zum Message-Log hinzu (sofern vorhanden).
 * ---------------------------------------------------------------------------- */
private void logPayload(def messageLog, String name, String content) {
    try {
        messageLog?.addAttachmentAsString(name, content, "application/json")
    } catch (Exception ignore) {
        /* Logging darf den Prozess niemals stoppen                              */
    }
}

/* ----------------------------------------------------------------------------
 * Zentrales Error-Handling. Fügt Payload als Attachment bei und wirft Exception.
 * ---------------------------------------------------------------------------- */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im Forecast-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}