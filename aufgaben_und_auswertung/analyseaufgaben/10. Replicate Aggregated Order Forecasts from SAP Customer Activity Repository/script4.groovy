/**************************************************************************
 * Groovy-Skript: Replicate Forecast – Request Mapping & API-Aufruf
 * Beschreibung:
 *  • Liest die Konfigurations-Properties/-Header
 *  • Führt – je nach Konfiguration – ein JSON-Mapping durch
 *  • Erstellt einen POST-Request an den SAP Customer Activity-Endpunkt
 *  • Fügt bei aktiviertem Logging das Request-Payload als Attachment hinzu
 *  • Vollständiges Error-Handling mit Payload-Anhang
 **************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import java.text.SimpleDateFormat
import java.util.TimeZone
import java.net.HttpURLConnection

// Haupteinstiegspunkt des Skriptes
def Message processData(Message message) {

    // MessageLog holen (für Monitoring/Attachments)
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        // 1. Konfiguration (Header & Properties) einlesen
        def cfg = readConfiguration(message)

        // 2. Original-Body laden
        String inboundBody = message.getBody(String) ?: ''

        // 3. Mapping – falls aktiviert
        String requestBody = cfg.enableMapping
                ? performRequestMapping(inboundBody, cfg.forecastCodeMap, messageLog)
                : inboundBody

        // 4. Logging – falls aktiviert
        if (cfg.enableLogging) {
            addAttachment(messageLog, 'MappedRequest', requestBody, 'application/json')
        }

        // 5. API-Aufruf
        int httpCode = callReplicateForecast(cfg.requestURL,
                                             cfg.requestUser,
                                             cfg.requestPassword,
                                             requestBody,
                                             messageLog)

        // 6. Technisches Ergebnis in Property schreiben
        message.setProperty('replicateForecast.httpCode', httpCode)

        // 7. Body für nachfolgende Schritte setzen
        message.setBody(requestBody)

    } catch (Exception e) {
        handleError(inboundBody, e, messageLog)
    }
    return message
}

/* ---------------------------------------------------------------------- *
 * Funktionsblöcke
 * ---------------------------------------------------------------------- */

/**
 * Liest erforderliche Header / Properties aus dem Message-Objekt.
 * Fehlt ein Wert, wird „placeholder“ verwendet, damit das Script nicht bricht.
 */
private Map readConfiguration(Message msg) {
    Map props = msg.getProperties()

    return [
        requestUser      : props.getOrDefault('requestUser',      'placeholder'),
        requestPassword  : props.getOrDefault('requestPassword',  'placeholder'),
        requestURL       : props.getOrDefault('requestURL',       'placeholder'),
        enableLogging    : (props.getOrDefault('enableLogging',   false ) as Boolean),
        enableMapping    : (props.getOrDefault('enableMapping',   true  ) as Boolean),
        // Forecast-Code-Map ist als verschachtelte Map hinterlegt
        forecastCodeMap  : (props.getOrDefault('forecastCodeMap', [:]).forecastCodeMap ?: [:])
    ]
}

/**
 * Führt das Request-Mapping anhand der Vorgaben durch.
 */
private String performRequestMapping(String body, Map fcMap, def msgLog) {

    // JSON einlesen
    def jsonIn = new JsonSlurper().parseText(body)

    // EntityVersion aufsplitten
    List<String> verParts = (jsonIn.entityVersion ?: '0.0.0').split('\\.')
    def entityVersionOut = [
            major: verParts[0] as int,
            minor: verParts[1] as int,
            patch: verParts[2] as int
    ]

    // Hilfs-Formatter
    def dfDate   = new SimpleDateFormat('dd.MM.yyyy HH:mm')
    def dfIsoUTC = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    dfDate.setTimeZone(TimeZone.getTimeZone('UTC'))
    dfIsoUTC.setTimeZone(TimeZone.getTimeZone('UTC'))

    // value-Array mappen
    def mappedValues = jsonIn.value.collect { item ->

        // Produkt-ID anpassen
        String productId = (item.product?.id ?: '').replaceFirst('^PROD-', 'PRODUCT-')

        // Werk-ID anreichern
        String plantId = 'PLANT-' + (item.supplyingPlant?.id ?: '')

        // Forecast-Code übersetzen
        String fcTranslated = fcMap.get(item.forecastCode) ?: item.forecastCode

        // Staging-Start formatieren
        def qtyArr = item.aggregatedOrderQuantities.collect { qty ->
            Date d = parseIsoDate(qty.productStagingStart as String)
            [productStagingStart: dfDate.format(d)]
        }

        // VersionTimestamp (Unix-Sekunden) in ISO-8601
        Date ts = new Date((item.versionTimestamp as long) * 1000)
        String tsIso = dfIsoUTC.format(ts)

        return [
                product                : [id: productId],
                supplyingPlant         : [id: plantId],
                forecastCode           : fcTranslated,
                aggregatedOrderQuantities: qtyArr,
                versionTimestamp       : tsIso
        ]
    }

    // Ziel-JSON aufbauen
    def jsonOut = [
            entityVersion: entityVersionOut,
            value        : mappedValues
    ]

    return new JsonBuilder(jsonOut).toPrettyString()
}

/**
 * POST-Aufruf „Replicate Forecasts“.
 */
private int callReplicateForecast(String urlStr,
                                  String user,
                                  String pwd,
                                  String payload,
                                  def msgLog) {

    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.with {
        requestMethod = 'POST'
        doOutput      = true
        setRequestProperty('Content-Type', 'application/json')
        String basicAuth = "${user}:${pwd}".bytes.encodeBase64().toString()
        setRequestProperty('Authorization', 'Basic ' + basicAuth)
        outputStream.withWriter('UTF-8') { it << payload }
    }

    int code = conn.responseCode
    msgLog?.addAttachmentAsString('HTTP-Status', String.valueOf(code), 'text/plain')
    // Response-Body wird laut Vorgabe nicht verarbeitet
    conn.disconnect()
    return code
}

/**
 * Fügt ein Attachment im SAP CI Monitoring hinzu.
 */
private void addAttachment(def msgLog, String name, String content, String mime) {
    msgLog?.addAttachmentAsString(name, content, mime)
}

/**
 * ISO-8601 / RFC-3339 → java.util.Date
 */
private Date parseIsoDate(String iso) {
    // SimpleDateFormat mit Zeitzonen-Support „X“
    return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").parse(iso)
}

/**
 * Zentrales Error-Handling:
 *  • Fehlermeldung und Payload als Attachment
 *  • Wirft RuntimeException, damit CPI die Exception erkennt
 */
private void handleError(String body, Exception e, def messageLog) {
    addAttachment(messageLog, 'ErrorPayload', body, 'application/json')
    String errMsg = "Fehler im Forecast-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}