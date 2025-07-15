/****************************************************************************************
* Name      : DayIO_Absence_Integration.groovy
* Autor     : ChatGPT – Senior Integration Developer
* Version   : 1.0
* Purpose   : Import von Abwesenheiten aus Day.IO in SAP SuccessFactors Employee Central
*             Payroll gem. Aufgabenstellung.
*
* WICHTIG   :  - Skript ist modular aufgebaut.
*             - Jede Methode besitzt deutschsprachige Kommentare.
*             - Vollständiges Error-Handling inkl. Attachment des Ursprungs-Payloads.
*             - Keine ungenutzten Imports, keine globalen Variablen/Konstanten.
****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.text.SimpleDateFormat
import java.util.Base64
import groovy.xml.MarkupBuilder

Message processData(Message message) {
    /* Einstiegspunkt  ******************************************************************/
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1) Header & Properties vorbereiten ********************************************/
        def ctx = initContext(message, messageLog)

        /* 2) Eingehenden JSON-Payload parsen *******************************************/
        def inputJson = new JsonSlurper().parseText(message.getBody(String) ?: '{}')
        List absences = inputJson?.request?.absences ?: []

        if (!absences) {
            throw new IllegalArgumentException('Es wurden keine Abwesenheiten übergeben.')
        }

        /* 3) Responses sammeln *********************************************************/
        List<Map> mappedResponses = []

        absences.each { Map absence ->
            /* 3.1) Request-Mapping */
            String requestXml = mapAbsenceToRequestXml(absence, ctx.tpOp)

            /* 3.2) Payload vor Aufruf loggen */
            addAttachment(messageLog, "RequestPayload_${absence.uniqueId}", requestXml, 'text/xml')

            /* 3.3) API-Aufruf */
            String responseXml = callSetAbsenceApi(ctx, requestXml)

            /* 3.4) Response-Mapping */
            Map mappedResponse = mapResponseToJson(responseXml, absence)
            mappedResponses << mappedResponse
        }

        /* 4) Alle Responses konsolidieren **********************************************/
        Map aggregated = [response: [absences: mappedResponses]]
        String outJson = JsonOutput.prettyPrint(JsonOutput.toJson(aggregated))
        message.setBody(outJson)

    } catch (Exception e) {
        handleError(message, e, messageLog)
    }

    return message
}

/* ==================================================================================== */
/* Initialisierung von Properties & Header ********************************************* */
private Map initContext(Message message, def messageLog) {
    /* Funktion: Liest benötigte Header/Properties aus dem Message-Objekt oder setzt
     *           "placeholder" als Fallback. Zusätzlich Ableitung des TP_OP-Wertes.     */

    def getVal = { String key ->
        def val = message.getProperty(key) ?: message.getHeader(key, String)
        (val == null || val.toString().trim().isEmpty()) ? 'placeholder' : val.toString()
    }

    def ctx = [
        user      : getVal('requestUser'),
        pwd       : getVal('requestPassword'),
        url       : getVal('requestURL'),
        tpOp      : deriveTpOp(getVal('p_operacao'))
    ]

    /* Für Transparenz alles noch einmal loggen */
    messageLog?.addAttachmentAsString('InitContext', ctx.toString(), 'text/plain')

    return ctx
}

/* ==================================================================================== */
/* Ableitung des TP_OP Wertes *********************************************************** */
private String deriveTpOp(String propVal) {
    /* Wenn leer oder 'placeholder' -> 'I', sonst Originalwert */
    return (propVal?.equalsIgnoreCase('placeholder') || propVal?.trim()?.isEmpty()) ? 'I' : propVal
}

/* ==================================================================================== */
/* Logging-Helfer: Fügt Attachments hinzu ********************************************** */
private void addAttachment(def messageLog, String name, String content, String mime) {
    if (messageLog) {
        messageLog.addAttachmentAsString(name, content, mime)
    }
}

/* ==================================================================================== */
/* Request-Mapping ********************************************************************* */
private String mapAbsenceToRequestXml(Map absence, String tpOp) {
    /* Funktion: Erstellt das Zielformat gemäß Mapping-Anforderungen. */

    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)
    def dateFmtIn  = new SimpleDateFormat('yyyy-MM-dd')
    def dateFmtOut = new SimpleDateFormat('yyyy-MM-dd')
    def timeFmtIn  = new SimpleDateFormat('HH:mm')
    def timeFmtOut = new SimpleDateFormat('HH:mm:ss')

    xml.mkp.xmlDeclaration(version: '1.0', encoding: 'UTF-8')
    xml.ZHR_FGRP_0001('xmlns': 'urn:sap-com:document:sap:rfc:functions') {
        P2001 {
            item {
                PERNR(absence.externalId ?: '')
                BEGDA(dateFmtOut.format(dateFmtIn.parse(absence.startDate)))
                BEGUZ(timeFmtOut.format(timeFmtIn.parse(absence.startTime)))
                STDAZ(absence.hours ?: '')
            }
        }
        TP_OP(tpOp)
    }
    return writer.toString()
}

/* ==================================================================================== */
/* API-Aufruf ************************************************************************* */
private String callSetAbsenceApi(Map ctx, String payload) {
    /* Funktion: Führt HTTP-POST via java.net.* aus und gibt Response als String zurück. */
    URLConnection conn = new URL(ctx.url).openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    String auth = "${ctx.user}:${ctx.pwd}"
    conn.setRequestProperty('Authorization',
            'Basic ' + Base64.encoder.encodeToString(auth.getBytes('UTF-8')))
    conn.setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')

    conn.outputStream.withWriter('UTF-8') { it << payload }

    int rc = conn.responseCode
    if (rc != 200) {
        String errBody = conn.errorStream?.text
        throw new RuntimeException("HTTP-Fehler ${rc}: ${errBody}")
    }
    return conn.inputStream.getText('UTF-8')
}

/* ==================================================================================== */
/* Response-Mapping ******************************************************************** */
private Map mapResponseToJson(String responseXml, Map absence) {
    /* Funktion: Mapped SAP-Payroll-Response ins Ziel-JSON je Abwesenheit. */
    def root = new XmlSlurper().parseText(responseXml)
    def items = root.'ns1:RETURN'.'ns1:item'
    def first = items[0]

    String status  = first?.'ns1:STATUS'?.text()
    String message = first?.'ns1:MENSAGEM'?.text()

    return [
        externalId: absence.externalId,
        startDate : absence.startDate,
        uniqueId  : absence.uniqueId,
        status    : (status?.equalsIgnoreCase('S') ? 'SUCCESS' : 'ERROR'),
        message   : message
    ]
}

/* ==================================================================================== */
/* Zentrales Error-Handling ************************************************************ */
private void handleError(Message message, Exception e, def messageLog) {
    /* Funktion: Fehler loggen & Attachment des Ursprungs-Payloads, dann Exception werfen. */
    String originalBody = message.getBody(String)
    addAttachment(messageLog, 'ErrorPayload', originalBody ?: '', 'text/plain')
    def errMsg = "Fehler im DayIO-Absence-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}