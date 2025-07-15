/****************************************************************************************
*  SAP Cloud Integration – Day.IO -> SAP SuccessFactors Employee Central Payroll
*  --------------------------------------------------------------------------------------
*  1.  Liest ein JSON-Payload (mehrere results) aus der Message.
*  2.  Erstellt pro result ein XML gem. Ziel-Schema und ruft die „Set Results“-API (POST).
*  3.  Parst jede XML-Response, mappt sie in eine JSON-Struktur und fasst alle Responses
*      zu einem Objekt zusammen.
*  4.  Fügt den ursprünglichen Payload als Attachment hinzu.
*  5.  Umfassendes Error-Handling mit Attachment des fehlerhaften Payloads.
*
*  Hinweis:  Alle Funktionen sind modular aufgebaut und enthalten deutsche Kommentare.
****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import groovy.xml.MarkupBuilder
import java.text.SimpleDateFormat
import java.nio.charset.StandardCharsets

Message processData(Message message) {

    /*----------------------------------------------------------
     * 0. Hilfs-Objekte und ursprünglichen Payload sichern
     *---------------------------------------------------------*/
    def messageLog = messageLogFactory.getMessageLog(message)
    final String originalBody = message.getBody(String) ?: ''

    // Original-Payload als Attachment
    addLogAttachment(messageLog,
                     'OriginalPayload',
                     originalBody,
                     'application/json')

    try {
        /*------------------------------------------------------
         * 1. Incoming JSON parsen
         *-----------------------------------------------------*/
        Map input          = new JsonSlurper().parseText(originalBody) as Map
        List<Map> results  = input?.request?.results ?: []

        /*------------------------------------------------------
         * 2. Properties & Header Defaulting (einmalig)
         *-----------------------------------------------------*/
        Map<String,String> cfg = initContext(message)

        /*------------------------------------------------------
         * 3. Jede Result-Zeile verarbeiten
         *-----------------------------------------------------*/
        List<Map> responseList = []

        results.eachWithIndex { Map result, int index ->
            // Properties für das aktuelle Element setzen (für Mapping & Logging)
            message.setProperty('p_externalId', result.externalId)
            message.setProperty('p_startDate',  result.startDate)
            message.setProperty('p_wageType',   result.wageType)

            /*--------------------------------------------------
             * 3.1 Request-XML bauen
             *-------------------------------------------------*/
            String reqXml = buildRequestXml(result, cfg)

            /*--------------------------------------------------
             * 3.2 API-Call durchführen
             *-------------------------------------------------*/
            String respXml = callSetResults(reqXml, cfg, index, messageLog)

            /*--------------------------------------------------
             * 3.3 Response-Mapping
             *-------------------------------------------------*/
            Map mapped = mapResponse(respXml, result, cfg)

            responseList << mapped
        }

        /*------------------------------------------------------
         * 4. Aggregierte Response als JSON setzen
         *-----------------------------------------------------*/
        Map finalResponse = [response: [results: responseList]]
        message.setBody(new JsonBuilder(finalResponse).toPrettyString(),
                        'application/json')

        return message

    } catch (Exception e) {
        // zentrales Error-Handling
        handleError(originalBody, e, messageLog)
        return message   // wird nie erreicht – handleError wirft Exception
    }
}

/*--------------------------------------------------------------
*  initContext – Properties & Header mit Default „placeholder“
*-------------------------------------------------------------*/
def Map<String,String> initContext(Message msg) {

    def placeholders = { val -> (val == null || val.toString().trim().isEmpty())
                                        ? 'placeholder' : val.toString() }

    return [
        'requestUser' : placeholders(msg.getProperty('requestUser')
                                   ?: msg.getHeader('requestUser',  String)),
        'requestPassword': placeholders(msg.getProperty('requestPassword')
                                   ?: msg.getHeader('requestPassword', String)),
        'requestURL' : placeholders(msg.getProperty('requestURL')
                                   ?: msg.getHeader('requestURL',  String)),
        'p_operacao' : placeholders(msg.getProperty('p_operacao')
                                   ?: msg.getHeader('p_operacao',  String))
    ]
}

/*--------------------------------------------------------------
*  buildRequestXml – Request-Mapping JSON -> XML
*-------------------------------------------------------------*/
def String buildRequestXml(Map result, Map cfg) {

    // Datumsformatierung
    final String formattedDate = formatDate(result.startDate)

    final String tpOpValue = (cfg.p_operacao == 'placeholder') ? 'I' : cfg.p_operacao

    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)

    // Namespace „ns1“
    xml.'ns1:ZHR_FGRP_0001'('xmlns:ns1':'urn:sap-com:document:sap:rfc:functions') {
        'P2010' {
            'item' {
                'PERNR'(result.externalId)
                'SUBTY'(result.wageType)
                'BEGDA'(formattedDate)
                'ANZHL'(result.quantityHours)
            }
        }
        'TP_OP'(tpOpValue)
    }
    return sw.toString()
}

/*--------------------------------------------------------------
*  callSetResults – REST-Aufruf der Z-Funktion
*-------------------------------------------------------------*/
def String callSetResults(String reqXml,
                          Map cfg,
                          int index,
                          def messageLog) {

    // Attachment für Request (optional pro Loop)
    addLogAttachment(messageLog,
                     "Request_${index + 1}",
                     reqXml,
                     'text/xml')

    // HTTP-Aufruf
    URL url = new URL(cfg.requestURL)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.requestMethod  = 'POST'
    conn.doOutput       = true

    // Basic-Auth Header
    String basicAuth = "${cfg.requestUser}:${cfg.requestPassword}"
                         .getBytes(StandardCharsets.UTF_8)
                         .encodeBase64()
                         .toString()
    conn.setRequestProperty('Authorization',"Basic ${basicAuth}")
    conn.setRequestProperty('Content-Type','text/xml; charset=utf-8')

    // Body senden
    conn.outputStream.withWriter('UTF-8') { it << reqXml }

    // Response lesen
    int rc = conn.responseCode
    String respBody
    if (rc >= 200 && rc < 300) {
        respBody = conn.inputStream.getText('UTF-8')
    } else {
        respBody = conn.errorStream?.getText('UTF-8') ?: ''
        throw new RuntimeException("HTTP-Fehler ${rc} beim SetResults-Call.")
    }

    // Response als Attachment
    addLogAttachment(messageLog,
                     "Response_${index + 1}",
                     respBody,
                     'text/xml')

    return respBody
}

/*--------------------------------------------------------------
*  mapResponse – XML -> JSON Mapping
*-------------------------------------------------------------*/
def Map mapResponse(String respXml, Map originalResult, Map cfg) {

    def xml = new XmlSlurper().parseText(respXml)
    def returnItem = xml?.RETURN?.item?.first()

    String statusRaw = returnItem?.STATUS?.text() ?: ''
    String status
    switch (statusRaw) {
        case 'S': status = 'SUCCESS'; break
        case 'E': status = 'ERROR';   break
        default : status = 'UNKNOWN'
    }

    return [
        externalId : originalResult.externalId,
        startDate  : originalResult.startDate,
        wageType   : originalResult.wageType,
        status     : status,
        message    : returnItem?.MENSAGEM?.text() ?: ''
    ]
}

/*--------------------------------------------------------------
*  formatDate – Hilfsfunktion für Datumsformat
*-------------------------------------------------------------*/
def String formatDate(String inDate) {
    if (!inDate) { return '' }
    // Eingabe bereits yyyy-MM-dd? -> dann unverändert zurück
    if (inDate ==~ /\d{4}-\d{2}-\d{2}/) { return inDate }

    // Ansonsten konvertieren
    def inFmt  = new SimpleDateFormat('yyyy-MM-dd')
    return inFmt.format(Date.parse('yyyy-MM-dd', inDate))
}

/*--------------------------------------------------------------
*  addLogAttachment – Anhängen beliebiger Inhalte an MessageLog
*-------------------------------------------------------------*/
def void addLogAttachment(def messageLog,
                          String name,
                          String content,
                          String mime) {
    try {
        messageLog?.addAttachmentAsString(name, content ?: '', mime)
    } catch(Exception ignore) {
        // Attachment-Fehler dürfen nie die Verarbeitung stoppen
    }
}

/*--------------------------------------------------------------
*  handleError – zentrales Error-Handling
*-------------------------------------------------------------*/
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload',
                                      body ?: '',
                                      'text/plain')
    def errorMsg = "Fehler im Integration-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}