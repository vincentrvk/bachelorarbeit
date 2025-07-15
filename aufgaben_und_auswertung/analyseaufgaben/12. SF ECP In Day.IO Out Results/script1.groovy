/*****************************************************************************************
*  Skript:   DayIO->SAP EC-Payroll Integration – Results Import
*  Autor:    ChatGPT (Senior-Integration-Entwickler / Groovy-Experte)
*  Stand:    17.06.2025
*
*  Beschreibung:
*  Dieses Skript liest Day.IO-Ergebnisse (JSON) ein, bildet für jeden Datensatz
*  das benötigte RFC-XML, ruft die SAP-Schnittstelle „ZHR_FGRP_0001“ auf,
*  mappt die Antwort zurück in ein konsolidiertes JSON-Format und gibt dieses
*  als Message-Body zurück.
*
*  Wichtige Rahmenbedingungen:
*     • Modulare Struktur mit ausführlichen deutschsprachigen Kommentaren
*     • Umfassendes Error-Handling inkl. Payload-Attachment
*     • Keine globalen Variablen / Konstanten
*     • Keine unnötigen Imports
******************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat

/*****************************************************************************************
*  Zentrale Einstiegsmethode
******************************************************************************************/
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        /* ---- Vorbereitung ----------------------------------------------------------- */
        final String inPayload = message.getBody(String)                       // Eingangs-JSON
        logPayload(messageLog, 'InputPayload', inPayload)                     // Logging / Attachment

        Map context = resolveContext(message)                                 // Properties / Header ermitteln
        List<Map> resultsIn = parseInputJson(inPayload)                       // JSON → Groovy-Objekte

        /* ---- Hauptverarbeitung: Loop über alle Day.IO-Ergebnisse -------------------- */
        List<Map> mappedResponses = []

        resultsIn.each { Map item ->
            // Properties, die im Response-Mapping benötigt werden, zur Laufzeit setzen
            setDynamicProperties(message, item)

            // Anfrage-XML erstellen
            String requestXml = mapRequest(item, context.p_operacao)

            // Anfrage loggen
            logPayload(messageLog, "Request_${item.externalId}", requestXml)

            // RFC-Aufruf
            Map callResult = callSetResults(requestXml, context, messageLog)

            // Antwort-Mapping
            Map mapped = mapResponse(callResult.body, message)
            mappedResponses << mapped

            // Antwort loggen
            logPayload(messageLog, "Response_${item.externalId}", callResult.body)
        }

        /* ---- Gesamtergebnis aufbereiten --------------------------------------------- */
        Map finalOut = [response: [results: mappedResponses]]
        String outJson = JsonOutput.toJson(finalOut)
        message.setBody(outJson)
        return message

    } catch (Exception e) {
        // Bei Fehler: Attachment & Exception re-throw
        handleError(message.getBody(String), e, messageLog)
    }
}

/*****************************************************************************************
*  Funktion: resolveContext
*  Liest benötigte Properties & Header aus dem Message-Objekt oder liefert 'placeholder'
******************************************************************************************/
private Map resolveContext(Message msg) {
    Map props  = msg.getProperties()
    Map hdrs   = msg.getHeaders()

    // Helfer-Closure
    def getVal = { String key -> (props[key] ?: hdrs[key]) ?: 'placeholder' }

    return [
        requestUser    : getVal('requestUser'),
        requestPassword: getVal('requestPassword'),
        requestURL     : getVal('requestURL'),
        p_operacao     : getVal('p_operacao')
    ]
}

/*****************************************************************************************
*  Funktion: parseInputJson
*  Wandelt den Eingangs-JSON-String in eine Liste von Maps um
******************************************************************************************/
private List<Map> parseInputJson(String jsonString) {
    def slurper = new JsonSlurper()
    Map parsed  = slurper.parseText(jsonString) as Map
    return (parsed.request?.results ?: []) as List<Map>
}

/*****************************************************************************************
*  Funktion: setDynamicProperties
*  Setzt pro Durchlauf Properties, die im Response-Mapping benötigt werden
******************************************************************************************/
private void setDynamicProperties(Message msg, Map item) {
    msg.setProperty('p_externalId', item.externalId ?: 'placeholder')
    msg.setProperty('p_startDate',  item.startDate  ?: 'placeholder')
    msg.setProperty('p_wageType',   item.wageType   ?: 'placeholder')
}

/*****************************************************************************************
*  Funktion: mapRequest
*  Erstellt das XML-Request-Payload basierend auf dem Mapping-Requirement
******************************************************************************************/
private String mapRequest(Map item, String pOperacao) {
    StringWriter sw = new StringWriter()
    def xml = new MarkupBuilder(sw)

    // Datum formatieren (yyyy-MM-dd)
    String begda = formatDate(item.startDate)

    // TP_OP bestimmen
    String tpOp = (!pOperacao || pOperacao == 'placeholder') ? 'I' : pOperacao

    xml.'ns1:ZHR_FGRP_0001'('xmlns:ns1': 'urn:sap-com:document:sap:rfc:functions') {
        P2010 {
            item {
                PERNR item.externalId
                SUBTY item.wageType
                BEGDA begda
                ANZHL item.quantityHours
            }
        }
        TP_OP(tpOp)
    }
    return sw.toString()
}

/*****************************************************************************************
*  Funktion: callSetResults
*  Führt den HTTP-POST Call gegen SAP aus und liefert Status-Code & Response-Body zurück
******************************************************************************************/
private Map callSetResults(String payload, Map ctx, def messageLog) {
    HttpURLConnection conn = null
    try {
        URL url = new URL(ctx.requestURL)
        conn = (HttpURLConnection) url.openConnection()
        conn.setRequestMethod('POST')
        conn.setDoOutput(true)
        conn.setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')

        // Basic-Auth Header
        String auth = "${ctx.requestUser}:${ctx.requestPassword}"
        String encAuth = auth.getBytes(StandardCharsets.UTF_8).encodeBase64().toString()
        conn.setRequestProperty('Authorization', "Basic ${encAuth}")

        // Body schreiben
        conn.outputStream.withWriter('UTF-8') { it << payload }

        int rc = conn.responseCode
        String responseBody = conn.inputStream.getText('UTF-8')
        return [code: rc, body: responseBody]

    } catch (Exception e) {
        handleError(payload, e, messageLog)
    } finally {
        conn?.disconnect()
    }
}

/*****************************************************************************************
*  Funktion: mapResponse
*  Mappt die SAP-Antwort (XML) in das definierte JSON-Zielformat
******************************************************************************************/
private Map mapResponse(String respXml, Message msg) {
    def slurper = new XmlSlurper().parseText(respXml)
    def itm     = slurper.RETURN?.item?.first()

    String statusSap = itm?.STATUS?.text() ?: ''
    String statusOut = (statusSap == 'S') ? 'SUCCESS' : 'ERROR'

    return [
        externalId: msg.getProperty('p_externalId'),
        startDate : msg.getProperty('p_startDate'),
        wageType  : msg.getProperty('p_wageType'),
        status    : statusOut,
        message   : itm?.MENSAGEM?.text() ?: ''
    ]
}

/*****************************************************************************************
*  Funktion: formatDate
*  Wandelt ein Datums-String in das Zielformat 'yyyy-MM-dd'
******************************************************************************************/
private String formatDate(String src) {
    if (!src) { return '' }
    // Eingabe ist bereits im gewünschten Format? → unverändert zurück
    if (src ==~ /\d{4}-\d{2}-\d{2}/) { return src }
    Date d = Date.parse('yyyy-MM-dd', src)
    return new SimpleDateFormat('yyyy-MM-dd').format(d)
}

/*****************************************************************************************
*  Funktion: logPayload
*  Hängt Payloads als String-Attachment an die Message für Monitoring-Zwecke
******************************************************************************************/
private void logPayload(def messageLog, String name, String payload) {
    messageLog?.addAttachmentAsString(name, payload ?: '', 'text/plain')
}

/*****************************************************************************************
*  Funktion: handleError
*  Einheitliches Error-Handling – wirft RuntimeException nach Logging & Attachment
******************************************************************************************/
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    def errorMsg = "Fehler im Integration-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}