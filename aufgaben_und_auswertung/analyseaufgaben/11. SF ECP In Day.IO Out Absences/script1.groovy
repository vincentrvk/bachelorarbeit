/*****************************************************************************
 * Groovy-Skript für SAP Cloud Integration – Day.IO Absence Import
 * ---------------------------------------------------------------------------
 * Autor:  Senior-Developer AI
 * Datum:  2025-06-17
 *
 * Dieses Skript übernimmt folgende Aufgaben:
 *  1. Einlesen des JSON-Payloads mit mehreren Abwesenheiten
 *  2. Technische Vorbereitungen (Header & Properties, Logging)
 *  3. Request-Mapping (JSON ➜ XML)
 *  4. Aufruf der SuccessFactors-Schnittstelle pro Abwesenheit
 *  5. Response-Mapping (XML ➜ JSON) und Zusammenfassung der Ergebnisse
 *  6. Durchgehendes Error-Handling mit Log-Attachments
 ****************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.mapping.*
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import java.text.SimpleDateFormat
import java.net.HttpURLConnection
import java.util.Base64

// ========================= MAIN FUNCTION ===================================
Message processData(Message message) {

    /* ---------- Initiales Logging ---------- */
    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        logPayload(message, messageLog)                                   // Original-Payload als Attachment

        /* ---------- Header & Property Vorbereitung ---------- */
        def cfg = prepareConfiguration(message)                           // Map mit allen benötigten Werten

        /* ---------- Eingangs-JSON parsen ---------- */
        def requestJson = new JsonSlurper().parseText(message.getBody(String) ?: '{}')
        def absences    = requestJson?.request?.absences ?: []

        /* ---------- Verarbeitung je Abwesenheit ---------- */
        List<Map> aggregatedResponses = []

        absences.each { Map absence ->
            // Properties setzen, um sie im Response-Mapping zu verwenden
            message.setProperty('p_externalId', absence.externalId ?: 'placeholder')
            message.setProperty('p_startDate',  absence.startDate  ?: 'placeholder')
            message.setProperty('p_uniqueId',   absence.uniqueId   ?: 'placeholder')

            /* --- Request-Mapping --- */
            String requestXml = mapRequest(absence, cfg.p_operacao)

            /* --- API-Aufruf --- */
            String responseXml = callSetAbsences(cfg.requestURL, cfg.requestUser, cfg.requestPassword, requestXml, messageLog)

            /* --- Response-Mapping --- */
            Map responseJson = mapResponse(responseXml, message)

            aggregatedResponses << responseJson
        }

        /* ---------- Aggregierte Antwort erzeugen ---------- */
        Map finalResponse =
                [ response: [ absences: aggregatedResponses ] ]

        message.setBody(JsonOutput.prettyPrint(JsonOutput.toJson(finalResponse)))
        message.setHeader('Content-Type', 'application/json')

        return message

    } catch (Exception e) {
        handleError(message.getBody(String) ?: '', e, messageLog)          // zentrales Error-Handling
        return message                                                     // (unerreichbar, aber notwendig)
    }
}
// ========================= HELPER & MODULES ================================

/* --------------------------------------------------------------------------
 *  Header / Property Vorbelegung
 * ------------------------------------------------------------------------*/
private Map prepareConfiguration(Message message) {
    Map<String, Object> cfg = [:]

    /* Hilfsclosure für Default-Werte */
    def resolve = { String key ->
        def val = message.getProperty(key)
        return val ? val.toString() : 'placeholder'
    }

    cfg.requestUser     = resolve('requestUser')
    cfg.requestPassword = resolve('requestPassword')
    cfg.requestURL      = resolve('requestURL')
    cfg.p_operacao      = resolve('p_operacao')

    return cfg
}

/* --------------------------------------------------------------------------
 *  Request-Mapping (JSON ➜ XML)
 * ------------------------------------------------------------------------*/
private String mapRequest(Map absence, String pOperacao) {

    /* Datum & Uhrzeit formatieren */
    String date   = formatDate(absence.startDate ?: '')
    String time   = formatTime(absence.startTime ?: '')
    String hours  = formatHours(absence.hours    ?: '0')

    String operacao = (!pOperacao || pOperacao == 'placeholder') ? 'I' : pOperacao

    /* XML-Generierung */
    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)
    xml.'ZHR_FGRP_0001'('xmlns': 'urn:sap-com:document:sap:rfc:functions') {
        'P2001' {
            'item' {
                'PERNR' absence.externalId ?: ''
                'BEGDA' date
                'BEGUZ' time
                'STDAZ' hours
            }
        }
        'TP_OP' operacao
    }
    return sw.toString()
}

/* --------------------------------------------------------------------------
 *  API-Call
 * ------------------------------------------------------------------------*/
private String callSetAbsences(String urlStr, String user, String pwd, String body, def messageLog) {

    if (!urlStr || urlStr == 'placeholder')
        throw new IllegalArgumentException('requestURL ist nicht gesetzt!')

    URL url = new URL(urlStr)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')

    /* Basic-Auth Header */
    String basicAuth = Base64.encoder.encodeToString("${user}:${pwd}".bytes)
    conn.setRequestProperty('Authorization', "Basic ${basicAuth}")

    /* Request Body senden */
    conn.outputStream.withWriter('UTF-8') { it << body }

    /* Response einlesen */
    int rc = conn.responseCode
    InputStream is = (rc >= 200 && rc < 300) ? conn.inputStream : conn.errorStream
    String response = is?.getText('UTF-8') ?: ''

    messageLog?.addAttachmentAsString("Response_${System.currentTimeMillis()}", response, 'text/xml')
    return response
}

/* --------------------------------------------------------------------------
 *  Response-Mapping (XML ➜ JSON)
 * ------------------------------------------------------------------------*/
private Map mapResponse(String xmlBody, Message message) {

    if (!xmlBody) {
        return [  externalId: message.getProperty('p_externalId'),
                  startDate : message.getProperty('p_startDate'),
                  uniqueId  : message.getProperty('p_uniqueId'),
                  status    : 'ERROR',
                  message   : 'Leerer Response-Body' ]
    }

    def nss = new groovy.xml.Namespace('urn:sap-com:document:sap:rfc:functions', 'ns1')
    def parsed = new XmlSlurper().parseText(xmlBody)

    def firstItem = parsed[nss.RETURN]?.[nss.item]?.first()

    String status  = firstItem?."${nss.STATUS}"?.text()   ?: ''
    String messageTxt = firstItem?."${nss.MENSAGEM}"?.text() ?: ''

    return [
            externalId: message.getProperty('p_externalId'),
            startDate : message.getProperty('p_startDate'),
            uniqueId  : message.getProperty('p_uniqueId'),
            status    : status,
            message   : messageTxt
    ]
}

/* --------------------------------------------------------------------------
 *  Logging-Funktion
 * ------------------------------------------------------------------------*/
private void logPayload(Message message, def messageLog) {
    try {
        String body = message.getBody(String) ?: ''
        messageLog?.addAttachmentAsString('OriginalPayload', body, 'application/json')
    } catch (Exception e) {
        // Logging darf keine Verarbeitung verhindern – deshalb nur protokollieren
    }
}

/* --------------------------------------------------------------------------
 *  Zentrales Error-Handling
 * ------------------------------------------------------------------------*/
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Day.IO Absence Script: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

/* --------------------------------------------------------------------------
 *  Hilfsfunktionen zur Formatierung
 * ------------------------------------------------------------------------*/
private String formatDate(String input) {
    try {
        if (!input) return ''
        def sdfIn  = new SimpleDateFormat('yyyy-MM-dd')
        def sdfOut = new SimpleDateFormat('yyyy-MM-dd')
        return sdfOut.format(sdfIn.parse(input))
    } catch (Exception ig) { return input }
}

private String formatTime(String input) {
    try {
        if (!input) return ''
        return input.contains(':') ? (input.length() == 5 ? "${input}:00" : input) : input
    } catch (Exception ig) { return input }
}

private String formatHours(String input) {
    try {
        return String.format(Locale.US, "%.2f", new BigDecimal(input))
    } catch (Exception ig) { return input }
}