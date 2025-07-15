/******************************************************************************
 * Groovy-Skript für SAP Cloud Integration
 * Aufgabe: Import von Abwesenheiten (Day.IO) nach SAP Payroll
 * Autor:   CPI Groovy Sample – Senior Integration Developer
 * Version: 1.0
 *
 * Wichtige Hinweise:
 *  - Jede Funktion ist modular aufgebaut und deutsch kommentiert.
 *  - Fehler werden zentral über handleError(...) geworfen
 *    und der Ursprungspayload als Attachment weitergegeben.
 ******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import java.net.HttpURLConnection
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.XmlUtil

/*************************   Haupteinstieg   *********************************/
Message processData(Message message) {
    // Message Log Instanz holen
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1. Properties & Header lesen */
        Map<String, String> props = fetchProperties(message)

        /* 2. Request JSON einlesen */
        String body        = message.getBody(String) ?: ''
        def requestJSON    = new JsonSlurper().parseText(body)
        List absencesInput = requestJSON?.request?.absences ?: []

        /* 3. Für jede Abwesenheit Request bauen, senden, Response mappen */
        List<Map> mappedResponses = []

        absencesInput.each { Map absence ->
            String requestXML  = buildRequestXml(absence, props)                    // Mapping
            logPayload(messageLog, "Request_${absence.uniqueId}", requestXML)      // Logging
            String responseXML = callApi(requestXML, props, messageLog)             // API-Call
            Map mappedResp     = mapResponse(responseXML, absence, props)           // Response-Mapping
            mappedResponses   << mappedResp
        }

        /* 4. Responses aggregieren und als JSON an Message zurückgeben */
        Map finalResponseJSON = [response: [absences: mappedResponses]]
        message.setBody(JsonOutput.prettyPrint(JsonOutput.toJson(finalResponseJSON)))

        return message
    } catch (Exception e) {
        // Zentrales Fehler-Handling
        handleError(message.getBody(String) ?: '', e, messageLog)
    }
}

/*************************   Hilfsfunktionen   *******************************/

/* Liest benötigte Properties & Header aus dem Message-Objekt (Platzhalter bei fehlenden Werten) */
private Map<String, String> fetchProperties(Message message) {
    [
        requestUser    : message.getProperty('requestUser')    ?: 'placeholder',
        requestPassword: message.getProperty('requestPassword')?: 'placeholder',
        requestURL     : message.getProperty('requestURL')     ?: 'placeholder',
        p_operacao     : message.getProperty('p_operacao')     ?: 'placeholder',
        p_externalId   : message.getProperty('p_externalId')   ?: 'placeholder',
        p_startDate    : message.getProperty('p_startDate')    ?: 'placeholder',
        p_uniqueId     : message.getProperty('p_uniqueId')     ?: 'placeholder'
    ]
}

/* Erstellt das XML für einen einzelnen Absence-Datensatz */
private String buildRequestXml(Map absence, Map props) {
    String tpOp = (props.p_operacao == 'placeholder' || props.p_operacao.trim().isEmpty())
                  ? 'I'
                  : props.p_operacao

    // Zeit ins Format HH:mm:ss bringen
    String beguz = (absence.startTime ?: '').trim()
    if (beguz && beguz.size() == 5) { beguz += ':00' }                       // aus “08:00” -> “08:00:00”

    """<ns1:ZHR_FGRP_0001 xmlns:ns1="urn:sap-com:document:sap:rfc:functions">
        <P2001>
            <item>
                <PERNR>${XmlUtil.escapeXml(absence.externalId ?: '')}</PERNR>
                <BEGDA>${XmlUtil.escapeXml(absence.startDate ?: '')}</BEGDA>
                <BEGUZ>${XmlUtil.escapeXml(beguz)}</BEGUZ>
                <STDAZ>${XmlUtil.escapeXml(absence.hours ?: '')}</STDAZ>
            </item>
        </P2001>
        <TP_OP>${XmlUtil.escapeXml(tpOp)}</TP_OP>
    </ns1:ZHR_FGRP_0001>"""
}

/* Führt den HTTP-POST durch und liefert die Response als String zurück */
private String callApi(String payload, Map props, def messageLog) {
    /* Wenn URL ein Platzhalter ist, wird der Call übersprungen – sinnvoll bei Tests */
    if ('placeholder'.equalsIgnoreCase(props.requestURL)) {
        logPayload(messageLog, 'SkippedCall', 'URL ist placeholder – API-Aufruf übersprungen.')
        return """<ns1:ZHR_FGRP_0001Response xmlns:ns1="urn:sap-com:document:sap:rfc:functions">
                    <ns1:RETURN>
                        <ns1:item>
                            <ns1:STATUS>S</ns1:STATUS>
                            <ns1:MENSAGEM>Skipped call – placeholder URL</ns1:MENSAGEM>
                        </ns1:item>
                    </ns1:RETURN>
                  </ns1:ZHR_FGRP_0001Response>"""
    }

    HttpURLConnection conn
    try {
        conn = (HttpURLConnection) new URL(props.requestURL).openConnection()
        conn.with {
            requestMethod = 'POST'
            doOutput      = true
            setRequestProperty('Content-Type',  'text/xml; charset=UTF-8')
            String auth   = "${props.requestUser}:${props.requestPassword}".bytes.encodeBase64().toString()
            setRequestProperty('Authorization', "Basic $auth")
            outputStream.withWriter('UTF-8') { it << payload }
        }

        int rc = conn.responseCode
        String respPayload = (rc >= 200 && rc < 300)
                ? conn.inputStream.getText('UTF-8')
                : conn.errorStream?.getText('UTF-8') ?: ''

        if (!(rc >= 200 && rc < 300)) {
            throw new RuntimeException("HTTP-Fehler $rc – $respPayload")
        }

        logPayload(messageLog, "Response_${System.currentTimeMillis()}", respPayload)
        return respPayload
    } finally {
        conn?.disconnect()
    }
}

/* Mapped eine einzelne SAP-Response zurück auf das gewünschte JSON */
private Map mapResponse(String respXml, Map absence, Map props) {
    def xml = new XmlSlurper().parseText(respXml)
    def firstItem = xml.depthFirst().find { it.name() == 'item' }

    String statusRaw = firstItem?.STATUS?.text() ?: ''
    String message   = firstItem?.MENSAGEM?.text() ?: ''

    String statusOut = statusRaw == 'S' ? 'SUCCESS'
                     : statusRaw == 'E' ? 'ERROR'
                     : statusRaw

    [
        externalId: absence.externalId ?: props.p_externalId,
        startDate : absence.startDate  ?: props.p_startDate,
        uniqueId  : absence.uniqueId   ?: props.p_uniqueId,
        status    : statusOut,
        message   : message
    ]
}

/* Fügt Payload/Information als Attachment ins MessageLog ein */
private void logPayload(def messageLog, String name, String content) {
    try {
        messageLog?.addAttachmentAsString(name, content, 'text/plain')
    } catch (Exception ignore) {
        /* Logging darf keinen Ablauf verhindern */
    }
}

/* Zentrales Fehler-Handling – wirft RuntimeException weiter */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}