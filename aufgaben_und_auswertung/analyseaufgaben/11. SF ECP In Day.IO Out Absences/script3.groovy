/********************************************************************************************************
*  Groovy-Script  : Day.IO –> SAP ECP Absence Import
*  Autor          : ChatGPT (Senior-Developer)
*  Beschreibung   : Liest Abwesenheiten aus dem eingehenden JSON-Payload, mappt jede Abwesenheit in
*                   das RFC-XML, ruft pro Abwesenheit die Payroll-Schnittstelle auf und fasst alle
*                   Antworten als JSON zusammen.
*********************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import groovy.xml.MarkupBuilder
import java.text.SimpleDateFormat
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.util.Base64

Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        /* -----------------------------------------------------------
         * 0) Body lesen & Grund-Logging
         * ----------------------------------------------------------- */
        final String incomingBody = message.getBody(String) ?: ''
        logPayload(messageLog, 'IncomingPayload', incomingBody)

        /* -----------------------------------------------------------
         * 1) Properties & Header lesen / setzen
         * ----------------------------------------------------------- */
        def ctx = resolveContext(message)

        /* -----------------------------------------------------------
         * 2) Eingehendes JSON parsen
         * ----------------------------------------------------------- */
        def jsonReq = new JsonSlurper().parseText(incomingBody)
        def absencesIn = (jsonReq?.request?.absences ?: [])

        /* -----------------------------------------------------------
         * 3) Antworten sammeln
         * ----------------------------------------------------------- */
        List<Map> responseList = []

        absencesIn.each { Map singleAbsence ->
            /* 3.1) Request-XML aufbauen */
            String xmlRequest = buildRequestXML(singleAbsence, ctx)

            /* 3.2) XML vor Aufruf loggen */
            logPayload(messageLog, "Request_${singleAbsence.uniqueId}", xmlRequest)

            /* 3.3) API-Call ausführen */
            String xmlResponse = callSetAbsence(ctx, xmlRequest, messageLog)

            /* 3.4) Antwort auswerten & mappen */
            Map mappedResponse = mapResponse(xmlResponse,
                                             singleAbsence.externalId as String,
                                             singleAbsence.startDate  as String,
                                             singleAbsence.uniqueId   as String)
            responseList << mappedResponse
        }

        /* -----------------------------------------------------------
         * 4) Alle Antworten zu einem JSON-Objekt zusammenführen
         * ----------------------------------------------------------- */
        Map finalResponse = [response: [absences: responseList]]
        String responseBody = new JsonBuilder(finalResponse).toPrettyString()

        /* -----------------------------------------------------------
         * 5) Ergebnis zurückgeben
         * ----------------------------------------------------------- */
        message.setBody(responseBody)
        return message

    } catch (Exception e) {
        handleError(message.getBody(String), e, messageLog)   // delegiert an zentrale Fehlerbehandlung
        return message   // (wird durch Exception eigentlich nie erreicht)
    }
}

/* **************************************************************************************************** *
 *                                       HILFSFUNKTIONEN                                                *
 * **************************************************************************************************** */

/* Kontext-Objekt mit allen benötigten Daten */
private Map resolveContext(Message msg) {
    /* Properties & Header auslesen – fehlende Felder mit 'placeholder' initialisieren */
    def prop = { name -> (msg.getProperty(name) ?: 'placeholder') as String }
    def head = { name -> (msg.getHeader(name, String.class) ?: 'placeholder') }

    return [
        user       : prop('requestUser'),
        password   : prop('requestPassword'),
        url        : prop('requestURL'),
        tpOp       : prop('p_operacao')
    ]
}

/* Erstellt das benötigte RFC-XML für genau eine Abwesenheit */
private String buildRequestXML(Map absence, Map ctx) {
    def writer = new StringWriter()
    def xml     = new MarkupBuilder(writer)
    xml.mkp.xmlDeclaration(version: '1.0', encoding: 'UTF-8')
    xml.'ns1:ZHR_FGRP_0001'('xmlns:ns1': 'urn:sap-com:document:sap:rfc:functions') {
        'P2001' {
            'item' {
                'PERNR'(absence.externalId)
                'BEGDA'(formatDate(absence.startDate))
                'BEGUZ'(formatTime(absence.startTime))
                'STDAZ'(absence.hours)
            }
        }
        'TP_OP'(ctx.tpOp && ctx.tpOp != 'placeholder' ? ctx.tpOp : 'I')
    }
    return writer.toString()
}

/* Ruft die Payroll-Schnittstelle auf – Basic-Auth POST */
private String callSetAbsence(Map ctx, String xmlBody, def messageLog) {
    HttpURLConnection conn = null
    try {
        URL url = new URL(ctx.url)
        conn = url.openConnection() as HttpURLConnection
        conn.with {
            requestMethod = 'POST'
            doOutput      = true
            connectTimeout = 15000
            readTimeout    = 15000
            setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')
            setRequestProperty('Authorization', 'Basic ' + base64("${ctx.user}:${ctx.password}"))
            outputStream.withWriter('UTF-8') { it << xmlBody }
        }

        int rc = conn.responseCode
        InputStream respStream = rc < 400 ? conn.inputStream : conn.errorStream
        String respBody = respStream?.getText('UTF-8') ?: ''

        messageLog?.addAttachmentAsString("RawResponse_${System.currentTimeMillis()}", respBody, 'text/xml')
        return respBody
    } catch (Exception ex) {
        throw new RuntimeException("HTTP-Call fehlgeschlagen: ${ex.message}", ex)
    } finally {
        conn?.disconnect()
    }
}

/* Antwort-XML -> Map  (gemäß Mapping-Anforderung) */
private Map mapResponse(String xmlResp, String externalId, String startDate, String uniqueId) {
    def slurper   = new XmlSlurper().parseText(xmlResp)
    def firstItem = slurper.'**'.find { it.name() == 'item' }   // erstes <item>-Element
    String statusSAP  = firstItem?.STATUS?.text() ?: ''
    String messageSAP = firstItem?.MENSAGEM?.text() ?: ''

    return [
        externalId: externalId,
        startDate : startDate,
        uniqueId  : uniqueId,
        status    : statusSAP == 'S' ? 'SUCCESS' : 'ERROR',
        message   : messageSAP
    ]
}

/* Datumsformatierung yyyy-MM-dd  */
private String formatDate(String src) {
    if (!src) return ''
    return Date.parse('yyyy-MM-dd', src).format('yyyy-MM-dd')
}
/* Zeitformatierung HH:mm:ss */
private String formatTime(String src) {
    if (!src) return ''
    Date d = Date.parse('HH:mm', src)
    return new SimpleDateFormat('HH:mm:ss').format(d)
}

/* Base64-Encoder ohne line breaks */
private static String base64(String plain) {
    return Base64.encoder.encodeToString(plain.getBytes(StandardCharsets.UTF_8))
}

/* Payload-Logging als Attachment */
private void logPayload(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content ?: '', 'text/plain')
}

/* Zentrale Fehlerbehandlung (gemäß Vorgabe) */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}