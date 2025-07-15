/*******************************************************************************************
 * SAP Cloud Integration – Groovy Skript                                                     
 * Replikation von Order Forecasts – Request-Mapping, Logging, HTTP-Aufruf & Error-Handling  
 *                                                                                           
 * Autor: AI-Assistent (Senior-Integrator)                                                   
 *                                                                                           
 * Wichtige Hinweise:                                                                        
 *  1. Das Skript ist strikt modular aufgebaut (getConfigValues, performRequestMapping, …).  
 *  2. Jede Methode ist auf Deutsch kommentiert.                                              
 *  3. Aussagekräftiges Error-Handling inkl. Attachment des fehlerhaften Payloads.           
 *  4. Unbenötigte Imports wurden vermieden.                                                  
 *******************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import java.time.format.DateTimeFormatter
import java.net.HttpURLConnection
import java.net.URL

// ------------------------------------------------------------
// Haupteinstiegspunkt des Skripts
// ------------------------------------------------------------
Message processData(Message message) {

    /* MessageLog für Monitor-Attachments */
    def messageLog = messageLogFactory.getMessageLog(message)

    /* Eingehenden Body als String zwischenspeichern (für Error-Handling) */
    String incomingBody = message.getBody(String) ?: ''

    try {
        /* 1. Konfigurationswerte (Properties & Header) ermitteln             */
        Map<String, Object> cfg = getConfigValues(message)

        /* 2. Optionales Request-Mapping                                     */
        String mappedBody = cfg.enableMapping ? performRequestMapping(incomingBody, cfg.forecastCodeMap)
                                              : incomingBody

        /* 3. Optionales Logging                                             */
        if (cfg.enableLogging) {
            logPayload(messageLog, 'RequestPayload', mappedBody)
        }

        /* 4. API-Aufruf „Replicate Forecasts“ mittels HTTP-POST             */
        int httpStatus = callReplicateForecasts(mappedBody,
                                                cfg.requestURL,
                                                cfg.requestUser,
                                                cfg.requestPassword,
                                                messageLog)

        /* 5. HTTP-Status in Header ablegen (zur weiteren Verwendung im Flow)*/
        message.setHeader('ReplicateForecastsHttpStatus', httpStatus)

        /* 6. Gemapptes JSON als neuen Body setzen                           */
        message.setBody(mappedBody)

        return message
    } catch (Exception e) {
        /* Zentrale Fehlerbehandlung                                          */
        handleError(incomingBody, e, messageLog)
        return message   // wird nie erreicht, handleError wirft Exception
    }
}

// ------------------------------------------------------------
//  Funktion: getConfigValues
// ------------------------------------------------------------
/**
 * Liest alle relevanten Properties/Headers aus dem Message-Objekt.
 * Fehlt ein Wert, wird „placeholder“ bzw. ein Default zurückgegeben.
 */
private Map<String, Object> getConfigValues(Message message) {

    Map props   = message.getProperties() ?: [:]
    Map header  = message.getHeaders()    ?: [:]

    /* Helper-Closure zum Zugreifen mit Fallback */
    def readValue = { Map src, String key ->
        src.containsKey(key) ? src[key] : 'placeholder'
    }

    /* enableLogging / enableMapping in Boolean umwandeln */
    boolean enableLogging = readValue(props, 'enableLogging').toString().toBoolean()
    boolean enableMapping = readValue(props, 'enableMapping').toString().toBoolean()

    /* forecastCodeMap kann Map oder JSON-String sein */
    def forecastCodeMapRaw = props['forecastCodeMap'] ?: [:]
    Map<String, String> forecastCodeMap = [:]
    if (forecastCodeMapRaw instanceof Map) {
        forecastCodeMap = forecastCodeMapRaw['forecastCodeMap'] ?: [:]
    } else if (forecastCodeMapRaw) {
        try {
            forecastCodeMap = new JsonSlurper()
                                .parseText(forecastCodeMapRaw.toString())
                                .forecastCodeMap ?: [:]
        } catch (ignored) {
            forecastCodeMap = [:]
        }
    }

    return [
        requestUser     : readValue(props, 'requestUser'),
        requestPassword : readValue(props, 'requestPassword'),
        requestURL      : readValue(props, 'requestURL'),
        enableLogging   : enableLogging,
        enableMapping   : enableMapping,
        forecastCodeMap : forecastCodeMap
    ]
}

// ------------------------------------------------------------
//  Funktion: performRequestMapping
// ------------------------------------------------------------
/**
 * Führt das im Fachkonzept beschriebene Mapping von INPUT → OUTPUT durch
 * und liefert einen JSON-String gemäß Zielschema zurück.
 */
private String performRequestMapping(String payload, Map<String, String> forecastCodeMap) {

    def inJson  = new JsonSlurper().parseText(payload)

    /* --- entityVersion aufteilen --------------------------------------- */
    def versionParts = inJson.entityVersion.toString().split('\\.')
    if (versionParts.size() != 3) {
        throw new IllegalArgumentException("Ungültiges entityVersion-Format: ${inJson.entityVersion}")
    }
    def outJson = [
        entityVersion: [
            major: versionParts[0].toInteger(),
            minor: versionParts[1].toInteger(),
            patch: versionParts[2].toInteger()
        ],
        value: inJson.value.collect { v ->
            /* Produkt-ID anpassen (PROD- → PRODUCT-) */
            String prodId = v.product.id.toString()
            if (prodId.startsWith('PROD-')) {
                prodId = prodId.replaceFirst('PROD-', 'PRODUCT-')
            }

            /* Werk-ID mit Prefix versehen */
            String plantId = v.supplyingPlant.id.toString()
            if (!plantId.startsWith('PLANT-')) {
                plantId = "PLANT-${plantId}"
            }

            /* Forecast-Code mappen */
            String fcDesc = forecastCodeMap?.get(v.forecastCode) ?: v.forecastCode

            /* productStagingStart umformatieren → DD.MM.YYYY HH:mm */
            DateTimeFormatter targetStageFmt =
                    DateTimeFormatter.ofPattern('dd.MM.yyyy HH:mm').withZone(ZoneId.of('UTC'))
            def aoqMapped = v.aggregatedOrderQuantities.collect { aoq ->
                def utcZdt = ZonedDateTime.parse(aoq.productStagingStart.toString())
                [productStagingStart: targetStageFmt.format(utcZdt)]
            }

            /* versionTimestamp (Unix-Sekunden) → ISO-8601 String in UTC */
            long epochSec = (v.versionTimestamp as Long)
            Instant inst  = Instant.ofEpochSecond(epochSec)
            String isoTs  = DateTimeFormatter
                                .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
                                .withZone(ZoneId.of('UTC'))
                                .format(inst)

            return [
                product                  : [id: prodId],
                supplyingPlant           : [id: plantId],
                forecastCode             : fcDesc,
                aggregatedOrderQuantities: aoqMapped,
                versionTimestamp         : isoTs
            ]
        }
    ]

    /* JSON formatiert zurückgeben */
    return JsonOutput.toJson(outJson)
}

// ------------------------------------------------------------
//  Funktion: logPayload
// ------------------------------------------------------------
/**
 * Hängt den übergebenen Inhalt als Attachment an das MessageLog an.
 */
private void logPayload(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content, 'application/json')
}

// ------------------------------------------------------------
//  Funktion: callReplicateForecasts
// ------------------------------------------------------------
/**
 * Führt den HTTP-POST gegen die Ziel-URL aus.
 * Gibt den HTTP-Statuscode zurück oder wirft im Fehlerfall eine Exception.
 */
private int callReplicateForecasts(String requestBody,
                                   String urlStr,
                                   String user,
                                   String password,
                                   def messageLog) {

    /* Platzhalter-URL → Aufruf überspringen (z. B. in Tests) */
    if ('placeholder'.equalsIgnoreCase(urlStr)) {
        logPayload(messageLog, 'SkippedHTTPCall', 'URL ist placeholder – HTTP-Aufruf wurde übersprungen.')
        return 0
    }

    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Content-Type', 'application/json')
    String basicAuth = "${user}:${password}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${basicAuth}")

    /* Request-Body senden */
    conn.outputStream.withWriter('UTF-8') { it << requestBody }

    int rc = conn.responseCode
    messageLog?.addAttachmentAsString('HTTP-Status', rc.toString(), 'text/plain')

    /* Fehlerhafte HTTP-Antworten abfangen */
    if (rc >= 400) {
        String errBody = conn.errorStream ? conn.errorStream.getText('UTF-8') : 'no error body'
        throw new RuntimeException("HTTP-Aufruf fehlgeschlagen (Status ${rc}): ${errBody}")
    }
    return rc
}

// ------------------------------------------------------------
//  Funktion: handleError
// ------------------------------------------------------------
/**
 * Zentrales Error-Handling gem. Vorgabe – Payload als Attachment &
 * aussagekräftige Fehlermeldung.
 */
private void handleError(String body, Exception e, def messageLog) {
    /* Payload für Analyse anhängen */
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'application/json')
    /* Exception eskalieren */
    throw new RuntimeException("Fehler im Groovy-Skript: ${e.message}", e)
}