/****************************************************************************************
 *  Groovy–Skript:  Day.IO → SAP SuccessFactors EC-Payroll – Result Import
 *  Beschreibung:
 *  -   Liest ein eingehendes JSON mit Arbeitszeit-Ergebnissen (request.results)
 *  -   Erstellt pro Resultat ein XML-Request                         (REQUEST-MAPPING)
 *  -   Ruft den RFC-Wrapper „ZHR_FGRP_0001“ via HTTP-POST auf       (API-CALL)
 *  -   Parst die RFC-Antwort, führt das RESPONSE-MAPPING aus
 *  -   Konsolidiert alle Antworten in eine JSON-Struktur            (Gewünschter Output)
 *  -   Umfasst ausführliches Logging, Fehlermanagement & Attachments
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.gateway.ip.core.customdev.util.MessageLog
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets

/* ==================================================================================== */
/*                               zentrale Prozess-Methode                               */
/* ==================================================================================== */
Message processData(Message message) {
    MessageLog mLog = messageLogFactory.getMessageLog(message)          // Log-Instanz
    String inboundBody = message.getBody(String) ?: ''                  // Original-Payload

    try {

        //----------------------------------------------------------------------
        // 1) Initiale Logs & Property/Header-Pflege
        //----------------------------------------------------------------------
        logPayload(mLog, 'IncomingPayload', inboundBody)                // Eingangs-Payload anhängen
        Map callCtx = setHeadersAndProperties(message, inboundBody)     // Properties / Header

        //----------------------------------------------------------------------
        // 2) JSON parsen
        //----------------------------------------------------------------------
        def reqJson = new JsonSlurper().parseText(inboundBody)
        def results  = reqJson.request?.results ?: []

        //----------------------------------------------------------------------
        // 3) Iteration über alle Result-Einträge
        //----------------------------------------------------------------------
        List<Map> aggregatedResponses = []

        results.each { Map singleResult ->

            /* 3.1  Properties, die vom Mapping abhängen, dynamisch anpassen     */
            message.setProperty('p_externalId', singleResult.externalId ?: 'placeholder')
            message.setProperty('p_startDate' , singleResult.startDate  ?: 'placeholder')
            message.setProperty('p_wageType'  , singleResult.wageType   ?: 'placeholder')

            /* 3.2  Request-XML erzeugen                                         */
            String xmlRequest = mapRequestToXml(singleResult, callCtx)

            /* 3.3  API-Call ausführen                                           */
            String xmlResponse = callSetResultsAPI(xmlRequest, callCtx, mLog)

            /* 3.4  Response mappen und sammeln                                  */
            Map mappedResponse = mapResponse(
                    xmlResponse,
                    singleResult.externalId,
                    singleResult.startDate,
                    singleResult.wageType
            )
            aggregatedResponses << mappedResponse
        }

        //----------------------------------------------------------------------
        // 4) Gesamt-Antwort aufbereiten & zurückgeben
        //----------------------------------------------------------------------
        String finalBody = JsonOutput.prettyPrint(JsonOutput.toJson([response:[results: aggregatedResponses]]))
        message.setBody(finalBody)
        message.setHeader('Content-Type', 'application/json')
        return message

    } catch (Exception e) {                                              // zentrales Error-Handling
        handleError(inboundBody, e, mLog)
        return message                                                   // Compiler-Beruhigung
    }
}

/* ==================================================================================== */
/*                                Hilfs-/Modul-Funktionen                               */
/* ==================================================================================== */

/* -----------------------------------------------------------------------
 * Schreibt Payload als Attachment ins Message-Log (Monitoring)
 * ---------------------------------------------------------------------*/
void logPayload(MessageLog mLog, String name, String payload) {
    if (mLog) {
        mLog.addAttachmentAsString(name ?: 'Payload', payload ?: '', 'text/plain')
    }
}

/* -----------------------------------------------------------------------
 * Liest vorhandene Header/Properties oder setzt „placeholder“
 * Rückgabe: Map mit allen benötigten Daten für den API-Aufruf
 * ---------------------------------------------------------------------*/
Map setHeadersAndProperties(Message msg, String body) {

    Map<String, Object> props = [:]

    /* Properties, die ggf. bereits existieren, übernehmen - sonst fallback */
    props.requestUser  = (msg.getProperty('requestUser' )) ?: 'placeholder'
    props.requestPwd   = (msg.getProperty('requestPassword')) ?: 'placeholder'
    props.requestUrl   = (msg.getProperty('requestURL'  )) ?: 'placeholder'
    props.p_operacao   = (msg.getProperty('p_operacao'  )) ?: 'placeholder'

    /* Header-Beispiel (könnte analog erweitert werden) */
    props.someHeader   = (msg.getHeader('SomeHeader', String)) ?: 'placeholder'

    return props
}

/* -----------------------------------------------------------------------
 * REQUEST-MAPPING: JSON-> XML  (ein Result-Eintrag)
 * ---------------------------------------------------------------------*/
String mapRequestToXml(Map item, Map ctx) {

    /* Datum konvertieren => yyyy-MM-dd                                         */
    String begda = ''
    if (item.startDate) {
        begda = Date.parse('yyyy-MM-dd', item.startDate).format('yyyy-MM-dd')
    }

    /* TP_OP gem. Regel wenn leer/placeholder                                  */
    String tpOp = (!ctx.p_operacao || ctx.p_operacao == 'placeholder') ? 'I' : ctx.p_operacao

    /* XML via MarkupBuilder generieren                                        */
    StringWriter sw = new StringWriter()
    MarkupBuilder mb = new MarkupBuilder(sw)

    mb.'ZHR_FGRP_0001'(xmlns: 'urn:sap-com:document:sap:rfc:functions') {
        P2010 {
            item {
                PERNR item.externalId ?: ''
                SUBTY item.wageType   ?: ''
                BEGDA begda
                ANZHL item.quantityHours ?: ''
            }
        }
        TP_OP tpOp
    }
    return sw.toString()
}

/* -----------------------------------------------------------------------
 * Tatsächlicher HTTP-POST gegen SAP Endpunkt (RFC-Wrapper)
 * ---------------------------------------------------------------------*/
String callSetResultsAPI(String xmlPayload, Map ctx, MessageLog mLog) {

    URL url   = new URL(ctx.requestUrl)
    String auth = "${ctx.requestUser}:${ctx.requestPwd}".bytes.encodeBase64().toString()

    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Authorization', "Basic ${auth}")
    conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')

    /* Request-Body schreiben */
    conn.outputStream.withWriter('UTF-8') { it << xmlPayload }

    int respCode = conn.responseCode
    String respBody

    if (respCode == HttpURLConnection.HTTP_OK ||
        respCode == HttpURLConnection.HTTP_CREATED ||
        respCode == HttpURLConnection.HTTP_ACCEPTED) {

        respBody = conn.inputStream.getText('UTF-8')
        logPayload(mLog, 'API-Response', respBody)                         // Response loggen

    } else {                                                               // Fehlerbehandlung
        String errorStream = conn.errorStream ? conn.errorStream.getText('UTF-8') : ''
        throw new RuntimeException("HTTP-Error ${respCode}: ${errorStream}")
    }
    return respBody
}

/* -----------------------------------------------------------------------
 * RESPONSE-MAPPING: XML-> JSON (Map)
 * ---------------------------------------------------------------------*/
Map mapResponse(String xmlResponse, String extId, String startDate, String wageType) {

    def respSlurp = new XmlSlurper().parseText(xmlResponse)
    def returnItem = respSlurp.RETURN.item[0]

    String statusCode = returnItem.STATUS.text() ?: ''
    String status     = (statusCode == 'S') ? 'SUCCESS' : 'FAILED'

    return [
            externalId : extId      ?: 'placeholder',
            startDate  : startDate  ?: 'placeholder',
            wageType   : wageType   ?: 'placeholder',
            status     : status,
            message    : returnItem.MENSAGEM.text() ?: ''
    ]
}

/* -----------------------------------------------------------------------
 * Einheitliches Error-Handling
 *    - schreibt fehlerhaften Payload als Attachment
 *    - wirft RuntimeException für die CPI-OnError-Schritte
 * ---------------------------------------------------------------------*/
void handleError(String body, Exception e, MessageLog mLog) {
    mLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}