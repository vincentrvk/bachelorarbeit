/***********************************************************************
*  Groovy-Skript für SAP CPI – Day.IO → EC-Payroll (SuccessFactors)
*
*  Umsetzung sämtlicher Anforderungen aus der Aufgabenstellung:
*   • separate, klar kommentierte Funktionen
*   • vollständiges Error-Handling inkl. Log-Attachment
*   • Payload-Logging vor jedem Aufruf
*   • Request- und Response-Mapping
************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import groovy.xml.MarkupBuilder
import groovy.xml.XmlSlurper
import java.text.SimpleDateFormat
import java.util.Base64

// =============================== H A U P T P R O Z E S S ===============================
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)      // Monitoring-Log
    def originPayload = message.getBody(String) ?: ''              // eingehender Payload (für Error-Handling)

    try {

        /* 1. Kontext-Initialisierung (Header / Properties lesen oder setzen) */
        def ctx = configureContext(message)

        /* 2. Eingehenden JSON-Payload parsen */
        def jsonReq = new JsonSlurper().parseText(originPayload)
        if(!jsonReq?.request?.results) {
            throw new IllegalArgumentException('Eingehender Payload enthält keine request.results-Struktur')
        }

        /* 3. Iteration über alle Day.IO-Ergebnisse                                      *
         *    – pro Ergebnis: Mapping → Aufruf → Response-Mapping → Sammeln in Liste     */
        List<Map> responseList = []
        jsonReq.request.results.each { Map singleResult ->

            /* 3.1 dynamische Properties für das einzelne Result (für spätere Response) */
            updateResultProperties(message, ctx, singleResult)

            /* 3.2 Request-Mapping (JSON → XML) */
            String requestXml = mapRequest(singleResult, ctx)

            /* 3.3 Logging vor dem Request                                              */
            logAttachment(message, "RequestPayload_${singleResult.externalId}", requestXml, 'text/xml')

            /* 3.4 API-Aufruf                                                           */
            String apiResponseXml = callSetResults(ctx, requestXml)

            /* 3.5 Response-Mapping (XML → JSON)                                        */
            Map mappedResp = mapResponse(apiResponseXml, singleResult)
            responseList << mappedResp
        }

        /* 4. Konsolidierten JSON-Response erzeugen und im Message-Body ablegen          */
        Map responseWrapper = [response:[results: responseList]]
        String responseJson = new JsonBuilder(responseWrapper).toPrettyString()
        message.setBody(responseJson)

        return message
    } catch(Exception e) {
        /* Fehlerbehandlung gem. Vorgabe */
        handleError(originPayload, e, messageLog)
        return message   // wird eigentlich nie erreicht – Exception wird erneut geworfen
    }
}

// =============================== F U N K T I O N E N ===============================

/* -------------------------------------------------------------------
 *  configureContext
 * -------------------------------------------------------------------
 *  Liest benötigte Header & Properties oder setzt sie auf 'placeholder'.
 *  Rückgabe: Map mit allen konfigurationsrelevanten Werten.
 */
def configureContext(Message message) {
    def getVal = { String key ->
        message.getProperty(key)?.toString() ?: message.getHeader(key, String)?.toString() ?: 'placeholder'
    }

    def ctx = [
        requestUser    : getVal('requestUser'),
        requestPassword: getVal('requestPassword'),
        requestURL     : getVal('requestURL'),
        p_operacao     : getVal('p_operacao')
    ]

    /* Fehlende Properties im Message-Objekt ergänzen, damit Down-Stream erreichbar */
    ctx.each { k,v -> if(message.getProperty(k) == null) message.setProperty(k, v) }

    return ctx
}

/* -------------------------------------------------------------------
 *  updateResultProperties
 * -------------------------------------------------------------------
 *  Setzt Ergebnisse bezogen auf EIN result als Properties,
 *  damit sie im nachfolgenden Response-Mapping verfügbar sind.
 */
def updateResultProperties(Message message, Map ctx, Map singleResult) {
    message.setProperty('p_externalId', singleResult.externalId ?: 'placeholder')
    message.setProperty('p_startDate',  singleResult.startDate  ?: 'placeholder')
    message.setProperty('p_wageType',   singleResult.wageType   ?: 'placeholder')
}

/* -------------------------------------------------------------------
 *  mapRequest
 * -------------------------------------------------------------------
 *  Erstellt das benötigte XML-Payload für den Aufruf.
 */
String mapRequest(Map src, Map ctx) {

    /* Datum formatieren (yyyy-MM-dd) */
    String formattedDate = src.startDate ?: ''
    if(formattedDate) {
        formattedDate = new SimpleDateFormat('yyyy-MM-dd')
                            .format(Date.parse('yyyy-MM-dd', formattedDate))
    }

    /* Operation ermitteln gem. Regeln ('I' wenn leer oder 'placeholder') */
    String tpOp = (ctx.p_operacao && !ctx.p_operacao.equalsIgnoreCase('placeholder')) ? ctx.p_operacao : 'I'

    /* XML erzeugen */
    StringWriter writer = new StringWriter()
    def builder = new MarkupBuilder(writer)
    builder.setDoubleQuotes(true)
    builder.'ns1:ZHR_FGRP_0001'('xmlns:ns1':'urn:sap-com:document:sap:rfc:functions') {
        P2010 {
            item {
                PERNR (src.externalId ?: '')
                SUBTY (src.wageType    ?: '')
                BEGDA (formattedDate   ?: '')
                ANZHL (src.quantityHours ?: '')
            }
        }
        TP_OP(tpOp)
    }
    return writer.toString()
}

/* -------------------------------------------------------------------
 *  callSetResults
 * -------------------------------------------------------------------
 *  Führt den HTTP-POST Call aus und liefert Response als String zurück.
 */
String callSetResults(Map ctx, String requestXml) {

    /* Verbindung initialisieren */
    def urlConn = new URL(ctx.requestURL).openConnection()
    urlConn.with {
        doOutput       = true
        requestMethod  = 'POST'
        connectTimeout = 30000
        readTimeout    = 30000

        // Basic-Auth Header
        def auth = "${ctx.requestUser}:${ctx.requestPassword}".bytes.encodeBase64().toString()
        setRequestProperty('Authorization', "Basic ${auth}")

        // Keine weiteren Header gem. Vorgaben
        outputStream.withWriter('UTF-8') { it << requestXml }
    }

    /* HTTP-Status prüfen */
    int rc = urlConn.responseCode
    if(rc != 200) {
        throw new RuntimeException("HTTP-Fehler beim Set Results Call – StatusCode: ${rc}")
    }

    /* Response lesen */
    return urlConn.inputStream.getText('UTF-8')
}

/* -------------------------------------------------------------------
 *  mapResponse
 * -------------------------------------------------------------------
 *  Mapped die XML-Response auf die geforderte JSON-Struktur.
 */
Map mapResponse(String responseXml, Map originalResult) {

    def xml = new XmlSlurper().parseText(responseXml)
    def itemNode = xml.RETURN.item[0]                           // gem. Schema mindestens ein item

    String statusSap = itemNode.STATUS.text() ?: ''
    String messageSap = itemNode.MENSAGEM.text() ?: ''

    /* Erfolgsklassifizierung (Business-Logik kann bei Bedarf angepasst werden) */
    String status = statusSap.equalsIgnoreCase('S') ? 'SUCCESS' : 'ERROR'

    /* Ziel-Map gem. Output-Schema */
    return [
        externalId: originalResult.externalId ?: '',
        startDate : originalResult.startDate  ?: '',
        wageType  : originalResult.wageType   ?: '',
        status    : status,
        message   : messageSap
    ]
}

/* -------------------------------------------------------------------
 *  logAttachment
 * -------------------------------------------------------------------
 *  Hängt einen String-Anhang an das Message-Objekt an.
 */
def logAttachment(Message message, String name, String content, String mimeType) {
    messageLogFactory.getMessageLog(message)
                     ?.addAttachmentAsString(name, content, mimeType ?: 'text/plain')
}

/* -------------------------------------------------------------------
 *  handleError
 * -------------------------------------------------------------------
 *  Einheitliche Fehlerbehandlung – wirft RuntimeException weiter.
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    def errorMsg = "Fehler im Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}