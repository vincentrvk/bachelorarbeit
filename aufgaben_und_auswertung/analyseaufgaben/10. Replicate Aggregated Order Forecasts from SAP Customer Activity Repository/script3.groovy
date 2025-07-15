/*****************************************************************************
*  Groovy-Skript: ReplicateForecasts.groovy
*  Beschreibung : Repliziert Forecast-Daten nach SAP CAO.
*  Autor        : AI – Senior Integration Engineer
*****************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import java.time.*
import java.time.format.DateTimeFormatter
import java.nio.charset.StandardCharsets

/* ================================================================
 *  Einstiegspunkt
 * ================================================================ */
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /*--- 1. Konfiguration laden --------------------------------------*/
        Map cfg = loadConfiguration(message)

        /*--- 2. Optional: Payload loggen ----------------------------------*/
        logPayload(cfg.enableLogging, messageLog, 'Payload_Before_Mapping', message.getBody(String))

        /*--- 3. Optional: Mapping ausführen -------------------------------*/
        if (cfg.enableMapping) {
            def mappedPayload = performRequestMapping(message.getBody(String), cfg.forecastCodeMap)
            message.setBody(mappedPayload)                               // Body ersetzen
        }

        /*--- 4. API-Call ausführen ---------------------------------------*/
        executeReplicateForecasts(message.getBody(String), cfg, messageLog)

        return message

    } catch (Exception e) {
        return handleError(message, e, messageLog)
    }
}

/* ================================================================
 *  Modul: Konfiguration
 * ================================================================ */
/**
 * Liest alle benötigten Header / Properties aus der Message.
 * Fehlt ein Eintrag, wird „placeholder“ verwendet.
 */
private Map loadConfiguration(Message msg) {

    def getVal = { key ->
        def val = msg.getProperty(key) ?: msg.getHeader(key, Object)
        (val == null || val.toString().trim().isEmpty()) ? 'placeholder' : val
    }

    Map forecastMapProp = [:]
    try {
        def raw = getVal('forecastCodeMap')
        if (raw && raw != 'placeholder') {
            forecastMapProp = new JsonSlurper().parseText(raw.toString())?.forecastCodeMap ?: [:]
        }
    } catch (Exception ignore) { /* Bei Fehler leere Map verwenden */ }

    /* Default-Map ergänzen, falls noch Keys fehlen */
    Map defaultForecastMap = [
            D: 'Daily forecast',
            W: 'Weekly forecast',
            M: 'Monthly forecast',
            Q: 'Quarterly forecast',
            A: 'Annual forecast'
    ]
    defaultForecastMap.each { k, v -> if (!forecastMapProp.containsKey(k)) forecastMapProp[k] = v }

    return [
            requestUser      : getVal('requestUser'),
            requestPassword  : getVal('requestPassword'),
            requestURL       : getVal('requestURL'),
            enableLogging    : getVal('enableLogging').toString().equalsIgnoreCase('true'),
            enableMapping    : getVal('enableMapping').toString().equalsIgnoreCase('true'),
            forecastCodeMap  : forecastMapProp
    ]
}

/* ================================================================
 *  Modul: Mapping
 * ================================================================ */
/**
 * Führt das im Pflichtenheft beschriebene Request-Mapping aus.
 * @param sourceJson  Ursprüngliches JSON als String
 * @param fcMap       ForecastCode-Mapping-Tabelle
 * @return            Gemapptes JSON als String
 */
private String performRequestMapping(String sourceJson, Map fcMap) {

    def source = new JsonSlurper().parseText(sourceJson)

    /*---------- entityVersion --------------------------------------*/
    def evSplit = (source.entityVersion ?: '0.0.0').tokenize('.')
    def entityVersion = [
            major: evSplit[0] as Integer,
            minor: evSplit[1] as Integer,
            patch: evSplit[2] as Integer
    ]

    /*---------- value-Array ----------------------------------------*/
    def valueArr = source.value.collect { v ->
        [
                product                 : [
                        id: transformProductId(v.product?.id)
                ],
                supplyingPlant          : [
                        id: "PLANT-${v.supplyingPlant?.id}"
                ],
                forecastCode            : mapForecastCode(v.forecastCode, fcMap),
                aggregatedOrderQuantities: v.aggregatedOrderQuantities?.collect { aoq ->
                    [
                            productStagingStart: convertToDottedDate(aoq.productStagingStart)
                    ]
                },
                versionTimestamp        : convertUnixTimestamp(v.versionTimestamp)
        ]
    }

    /*---------- Zielobjekt bauen -----------------------------------*/
    def target = [
            entityVersion: entityVersion,
            value        : valueArr
    ]

    return new JsonBuilder(target).toPrettyString()
}

/* Hilfs-Funktionen für das Mapping */
private String transformProductId(String srcId) {
    srcId ? srcId.replaceFirst(/^PROD-/, 'PRODUCT-') : ''
}

private String mapForecastCode(String code, Map m) {
    m.get(code ?: '') ?: code
}

private String convertToDottedDate(String isoTs) {
    if (!isoTs) return ''
    def dt = Instant.parse(isoTs).atZone(ZoneId.of('UTC'))
    return dt.format(DateTimeFormatter.ofPattern('dd.MM.yyyy HH:mm'))
}

private String convertUnixTimestamp(Number unixSeconds) {
    if (unixSeconds == null) return ''
    Instant.ofEpochSecond(unixSeconds.longValue())
           .atZone(ZoneId.of('UTC'))
           .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
}

/* ================================================================
 *  Modul: Logging
 * ================================================================ */
/**
 * Fügt dem MessageLog den angegebenen Payload als Attachment hinzu.
 */
private void logPayload(boolean enabled, def messageLog, String name, String payload) {
    if (enabled && messageLog) {
        messageLog.addAttachmentAsString(name, payload ?: '', 'application/json')
    }
}

/* ================================================================
 *  Modul: API-Call
 * ================================================================ */
/**
 * Führt den HTTP-POST zum Zielsystem aus.
 */
private void executeReplicateForecasts(String body,
                                       Map cfg,
                                       def messageLog) {

    if (cfg.requestURL == 'placeholder') {
        messageLog?.addAttachmentAsString('Warning', 'Kein gültiger requestURL übergeben – HTTP-Aufruf übersprungen.', 'text/plain')
        return
    }

    URL url = new URL(cfg.requestURL)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.with {
        requestMethod  = 'POST'
        doOutput       = true
        setRequestProperty('Content-Type', 'application/json')
        String auth    = "${cfg.requestUser}:${cfg.requestPassword}"
        String encAuth = auth.bytes.encodeBase64().toString()
        setRequestProperty('Authorization', "Basic ${encAuth}")
        outputStream.withWriter(StandardCharsets.UTF_8.name()) { it << body }
    }

    int rc = conn.responseCode
    if (rc >= 200 && rc < 300) {
        messageLog?.setStringProperty('ReplicateForecasts_ResponseCode', rc.toString())
    } else {
        String errorMsg = "HTTP-Fehler beim Aufruf von ReplicateForecasts – ResponseCode: $rc"
        throw new RuntimeException(errorMsg)
    }
}

/* ================================================================
 *  Modul: Error-Handling
 * ================================================================ */
/**
 * Einheitliches Error-Handling – wirft RuntimeException mit detaillierter Nachricht
 * und hängt den Ursprungs-Payload als Attachment an.
 */
private Message handleError(Message msg, Exception e, def messageLog) {
    try {
        String body = msg.getBody(String)
        messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'application/json')
        messageLog?.setStringProperty('GroovyError', e.getMessage())
    } catch (Exception ignore) { /* Keine Nebenwirkung bei Logging-Fehler */ }

    throw new RuntimeException("Fehler im Skript ReplicateForecasts: ${e.message}", e)
}
