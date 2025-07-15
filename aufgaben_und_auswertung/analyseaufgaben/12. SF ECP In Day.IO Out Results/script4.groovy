/****************************************************************************************
 *  SAP Cloud Integration – Groovy-Script
 *  Aufgabe: Day.IO Ergebnisse an SAP EC Payroll senden
 *  Autor  : ChatGPT (Senior Integration Developer)
 *  
 *  Wichtige Hinweise:
 *  • Jede Funktion ist modular aufgebaut und enthält deutschsprachige Kommentare.
 *  • Error-Handling schreibt den fehlerhaften Payload als Attachment und wirft
 *    anschließend eine RuntimeException.
 *  • Vor jedem HTTP-Request wird der Request-Payload als Attachment geloggt.
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import groovy.xml.MarkupBuilder
import java.text.SimpleDateFormat

// ================================================================================
//  Haupt-Einstiegspunkt
// ================================================================================
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* -----------------------------------------------------------
         * 1) Header & Properties ermitteln / setzen
         * -----------------------------------------------------------*/
        setHeadersAndProperties(message)

        /* -----------------------------------------------------------
         * 2) Eingehenden JSON-Body parsen
         * -----------------------------------------------------------*/
        def jsonIn  = new JsonSlurper().parseText(message.getBody(String) as String)
        def results = jsonIn?.request?.results ?: []
        def responseList = []

        /* -----------------------------------------------------------
         * 3) Für jedes Result einzeln verarbeiten
         * -----------------------------------------------------------*/
        results.each { Map res ->

            // Properties für dieses Result aktualisieren (werden im Response-Mapping gebraucht)
            message.setProperty("p_externalId", res.externalId ?: "placeholder")
            message.setProperty("p_startDate" , res.startDate  ?: "placeholder")
            message.setProperty("p_wageType"  , res.wageType   ?: "placeholder")

            /* ------- 3.1 Request-Mapping erzeugen ------------------*/
            String requestXML = buildRequestXML(res, message)

            /* ------- 3.2 Request-Payload loggen --------------------*/
            logPayload(
                    "RequestPayload_${res.externalId ?: 'unknown'}",
                    requestXML,
                    "text/xml",
                    messageLog
            )

            /* ------- 3.3 API-Call durchführen ----------------------*/
            String responseXML = callSetResultsAPI(requestXML, message)

            /* ------- 3.4 Response-Mapping --------------------------*/
            Map<String, Object> responseMapped = mapResponse(responseXML, message)

            responseList << responseMapped
        }

        /* -----------------------------------------------------------
         * 4) Aggregierte Antwort aufbereiten
         * -----------------------------------------------------------*/
        def finalJson = [response: [results: responseList]]
        String finalPayload = new JsonBuilder(finalJson).toPrettyString()
        message.setBody(finalPayload)

    } catch (Exception e) {
        // Fehler auf oberster Ebene abfangen
        handleError(message.getBody(String) as String, e, messageLog)
    }

    return message
}

// ================================================================================
//  Modul 1 – Header & Properties setzen
// ================================================================================
/**
 * Ermittelt vorhandene Header/Properties oder setzt Platzhalterwerte.
 */
void setHeadersAndProperties(Message message) {

    // Zu prüfende Properties mit Default "placeholder"
    [
        "requestUser",
        "requestPassword",
        "requestURL",
        "p_operacao"
    ].each { String key ->
        if (!message.getProperty(key)) {
            message.setProperty(key, "placeholder")
        }
    }

    // Die vier dynamischen Request-abhängigen Properties werden hier nur
    // vorbereitet – konkrete Werte werden vor jedem API-Call gesetzt.
    [
        "p_externalId",
        "p_startDate",
        "p_wageType"
    ].each { String key ->
        if (!message.getProperty(key)) {
            message.setProperty(key, "placeholder")
        }
    }
}

// ================================================================================
//  Modul 2 – Request-Mapping
// ================================================================================
/**
 * Erstellt das XML für einen einzelnen Result-Eintrag.
 */
String buildRequestXML(Map result, Message message) {

    // Datumsformat vereinheitlichen
    String begda = formatDate(result.startDate ?: "")

    // Operation bestimmen
    String tpOp = message.getProperty("p_operacao")
    if (!tpOp || tpOp.equalsIgnoreCase("placeholder")) {
        tpOp = "I"
    }

    // XML mittels MarkupBuilder erzeugen
    StringWriter sw = new StringWriter()
    MarkupBuilder xml = new MarkupBuilder(sw)
    xml.setDoubleQuotes(true)

    xml.'ns1:ZHR_FGRP_0001'('xmlns:ns1': 'urn:sap-com:document:sap:rfc:functions') {
        'ns1:P2010' {
            'ns1:item' {
                'ns1:PERNR'(result.externalId ?: '')
                'ns1:SUBTY'(result.wageType  ?: '')
                'ns1:BEGDA'(begda)
                'ns1:ANZHL'(result.quantityHours ?: '')
            }
        }
        'ns1:TP_OP'(tpOp)
    }

    return sw.toString()
}

// ================================================================================
//  Modul 3 – API-Call
// ================================================================================
/**
 * Führt den HTTP-POST Call gegen EC Payroll durch und liefert die Response zurück.
 */
String callSetResultsAPI(String payloadXml, Message message) {

    String urlStr  = message.getProperty("requestURL")
    String user    = message.getProperty("requestUser")
    String pwd     = message.getProperty("requestPassword")

    if (urlStr == "placeholder" || user == "placeholder" || pwd == "placeholder") {
        throw new IllegalStateException("Notwendige Verbindungsparameter (URL/User/Password) fehlen oder sind 'placeholder'.")
    }

    URL url = new URL(urlStr)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.requestMethod = "POST"
    conn.doOutput      = true
    conn.setRequestProperty("Content-Type", "application/xml")
    conn.setRequestProperty("Authorization", "Basic " + "${user}:${pwd}".bytes.encodeBase64().toString())

    // Request-Payload senden
    conn.outputStream.withWriter("UTF-8") { writer ->
        writer << payloadXml
    }

    int rc = conn.responseCode
    InputStream is = rc >= 200 && rc < 300 ? conn.inputStream : conn.errorStream
    String response = is?.getText("UTF-8") ?: ""

    if (rc < 200 || rc >= 300) {
        throw new RuntimeException("HTTP-Fehler beim Aufruf der Payroll Schnittstelle. ResponseCode=${rc}, Response=${response}")
    }

    return response
}

// ================================================================================
//  Modul 4 – Response-Mapping
// ================================================================================
/**
 * Mapped die XML-Response in ein JSON-kompatibles Map-Objekt.
 */
Map<String, Object> mapResponse(String responseXml, Message message) {

    def ns = new groovy.xml.Namespace('urn:sap-com:document:sap:rfc:functions','')
    def xml = new XmlSlurper().parseText(responseXml)

    // Erster RETURN/item Knoten
    def itemNode = xml[ns.RETURN]?.item?.getAt(0)

    String status   = itemNode?.STATUS?.text()   ?: ""
    String mensagem = itemNode?.MENSAGEM?.text() ?: ""

    return [
        externalId : message.getProperty("p_externalId") ?: "",
        startDate  : message.getProperty("p_startDate")  ?: "",
        wageType   : message.getProperty("p_wageType")   ?: "",
        status     : status,
        message    : mensagem
    ]
}

// ================================================================================
//  Modul 5 – Logging
// ================================================================================
/**
 * Hängt einen Payload als Attachment an das MessageLog an.
 */
void logPayload(String name, String payload, String mimeType, def messageLog) {
    messageLog?.addAttachmentAsString(name, payload, mimeType)
}

// ================================================================================
//  Modul 6 – Error-Handling
// ================================================================================
/**
 * Schreibt den fehlerhaften Payload als Attachment und wirft eine Exception.
 */
void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

// ================================================================================
//  Hilfsfunktionen
// ================================================================================
/**
 * Formatiert ein Datum in 'yyyy-MM-dd'. Unerwartete Formate werden unverändert zurückgegeben.
 */
String formatDate(String dateStr) {
    try {
        Date parsed = Date.parse("yyyy-MM-dd", dateStr)
        return new SimpleDateFormat("yyyy-MM-dd").format(parsed)
    } catch (Exception ignore) {
        // Wenn das Parsing fehlschlägt, Original zurückgeben
        return dateStr
    }
}