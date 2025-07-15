/*****************************************************************************
 * Groovy-Skript: ReplicateForecasts.groovy
 * Autor      : AI Senior-Entwickler
 * Datum      : 2025-06-16
 * Beschreibung:
 *  – Liest eingehenden JSON-Payload
 *  – Führt optionales Mapping nach Zielschema durch
 *  – Fügt optional Logging-Attachment hinzu
 *  – Sendet HTTP-POST an SAP Customer Activity „Replicate Forecasts“
 *  – Umfassendes Error-Handling inkl. Payload-Attachment
 *****************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets
import java.time.*
import java.time.format.DateTimeFormatter

/********************************* Hauptentry ********************************/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    try {

        /* 1. Header- & Property-Werte ermitteln                              */
        Map cfg = readConfiguration(message)

        /* 2. Body lesen                                                      */
        String incomingBody = message.getBody(String) ?: ''

        /* 3. Optionales Mapping                                              */
        String mappedBody = cfg.enableMapping
                ? performRequestMapping(incomingBody, cfg, messageLog)
                : incomingBody

        /* 4. Optionales Logging                                              */
        if (cfg.enableLogging) {
            addAttachment(messageLog,
                    'RequestPayload',
                    mappedBody,
                    'application/json')
        }

        /* 5. HTTP-Aufruf                                                     */
        int httpStatus = callReplicateForecasts(mappedBody, cfg, messageLog)
        message.setHeader('HTTP_RESPONSE_CODE', httpStatus)

        /* 6. Response-Body nicht relevant – Body bleibt gemappt              */
        message.setBody(mappedBody)
        return message

    } catch (Exception e) {
        // vollständiges Error-Handling inkl. Attachment
        handleError(message.getBody(String) ?: '', e, messageLog)
        // wird nie erreicht, handleError wirft Exception
        return message
    }
}

/*************************** Konfigurations-Helper ***************************/
/* Liest Properties & Header und befüllt mit Platzhaltern falls nötig       */
private Map readConfiguration(Message msg) {

    Map props          = msg.getProperties() ?: [:]
    Map headers        = msg.getHeaders()     ?: [:]

    Map cfg = [
            requestUser      : props.requestUser      ?: headers.requestUser      ?: 'placeholder',
            requestPassword  : props.requestPassword  ?: headers.requestPassword  ?: 'placeholder',
            requestURL       : props.requestURL       ?: headers.requestURL       ?: 'placeholder',
            enableLogging    : (props.enableLogging   ?: headers.enableLogging   ?: 'false')
                                .toString()
                                .equalsIgnoreCase('true'),
            enableMapping    : (props.enableMapping   ?: headers.enableMapping   ?: 'false')
                                .toString()
                                .equalsIgnoreCase('true'),
            forecastCodeMap  : extractForecastCodeMap(props.forecastCodeMap)
    ]
    return cfg
}

/* Wandelt die übergebene ForecastCode-Map in eine Map<String,String>       */
private Map extractForecastCodeMap(Object raw) {
    if (!raw) {
        return [:]
    }
    if (raw instanceof Map) {
        // Erwarteter CPI-Fall: Property bereits als Map
        return (Map) raw?.forecastCodeMap ?: (Map) raw
    }
    // Fallback: Property kommt als String (JSON)
    try {
        def json = new JsonSlurper().parseText(raw.toString())
        return (Map) json?.forecastCodeMap ?: (Map) json
    } catch (Exception ignored) {
        return [:]
    }
}

/****************************** Mapping-Logik *******************************/
/* Führt das Request-Mapping gem. Vorgaben aus                              */
private String performRequestMapping(String inBody, Map cfg, def messageLog) {

    def slurper = new JsonSlurper()
    def source  = slurper.parseText(inBody ?: '{}')

    // --- entityVersion ----------------------------------------------------
    List<Integer> verParts = []
    try {
        verParts = source.entityVersion?.toString()?.tokenize('.')?.collect { it as Integer }
        if (verParts?.size() != 3) {
            throw new IllegalArgumentException("entityVersion hat kein gültiges Format.")
        }
    } catch (Exception e) {
        throw new IllegalArgumentException("entityVersion Parsing fehlgeschlagen.", e)
    }

    def target = [
            entityVersion: [
                    major: verParts[0],
                    minor: verParts[1],
                    patch: verParts[2]
            ],
            value: []
    ]

    // --- value Array ------------------------------------------------------
    source.value?.each { item ->
        def tgtItem = [:]

        // ID-Mapping
        String prodId = item?.product?.id?.toString() ?: ''
        prodId = prodId.replaceFirst('^PROD-', 'PRODUCT-')

        tgtItem.product = [id: prodId]

        // SupplyingPlant-Mapping
        String plantId = item?.supplyingPlant?.id?.toString() ?: ''
        if (!plantId.startsWith('PLANT-')) {
            plantId = "PLANT-${plantId}"
        }
        tgtItem.supplyingPlant = [id: plantId]

        // ForecastCode-Mapping
        String originalCode = item?.forecastCode?.toString() ?: ''
        String mappedCode   = cfg.forecastCodeMap.get(originalCode, originalCode)
        tgtItem.forecastCode = mappedCode

        // AggregatedOrderQuantities
        List aggList = []
        item.aggregatedOrderQuantities?.each { aq ->
            String ts = aq?.productStagingStart?.toString() ?: ''
            String formatted = formatDateTime(ts,
                    'yyyy-MM-dd\'T\'HH:mm:ssX',
                    'dd.MM.yyyy HH:mm')
            aggList << [productStagingStart: formatted]
        }
        tgtItem.aggregatedOrderQuantities = aggList

        // versionTimestamp
        Long unix = (item?.versionTimestamp ?: 0) as Long
        String iso = Instant.ofEpochSecond(unix)
                .atZone(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
        tgtItem.versionTimestamp = iso

        target.value << tgtItem
    }

    return new JsonBuilder(target).toPrettyString()
}

/* Zeitformat-Konverter                                                    */
private String formatDateTime(String input,
                              String inPattern,
                              String outPattern) {

    DateTimeFormatter inFmt  = DateTimeFormatter.ofPattern(inPattern)
                                                .withZone(ZoneOffset.UTC)
    DateTimeFormatter outFmt = DateTimeFormatter.ofPattern(outPattern)
                                                .withZone(ZoneOffset.UTC)
    Instant instant = ZonedDateTime.parse(input, inFmt).toInstant()
    return outFmt.format(instant)
}

/***************************** HTTP-Aufruf **********************************/
/* Führt den POST-Call gegen die Ziel-API aus                              */
private int callReplicateForecasts(String payload,
                                   Map cfg,
                                   def messageLog) {

    URL url                   = new URL(cfg.requestURL)
    HttpURLConnection conn    = (HttpURLConnection) url.openConnection()

    conn.setRequestMethod('POST')
    conn.setRequestProperty('Content-Type', 'application/json')
    conn.setRequestProperty('Authorization',
            'Basic ' + "${cfg.requestUser}:${cfg.requestPassword}"
                    .bytes
                    .encodeBase64()
                    .toString())
    conn.setDoOutput(true)

    // Body schreiben
    conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { writer ->
        writer << payload
    }

    int responseCode = conn.responseCode
    addAttachment(messageLog,
            'HTTP_Response_Code',
            responseCode.toString(),
            'text/plain')

    if (responseCode >= 400) {
        String errorStream = conn.errorStream?.getText(StandardCharsets.UTF_8.name()) ?: ''
        throw new RuntimeException("HTTP-Error ${responseCode}: ${errorStream}")
    }
    return responseCode
}

/****************************** Logging-Helper ******************************/
/* Fügt ein Attachment hinzu, wenn messageLog existiert                    */
private void addAttachment(def messageLog,
                           String name,
                           String content,
                           String mime) {
    try {
        messageLog?.addAttachmentAsString(name, content, mime)
    } catch (Exception ignored) {
        // Logging darf den Flow nicht brechen
    }
}

/****************************** Error-Handling ******************************/
/* Gemäß vorgegebenem Snippet                                              */
private void handleError(String body, Exception e, def messageLog) {
    // Logging im Monitoring (Name, Inhalt, Typ)
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/plain')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}