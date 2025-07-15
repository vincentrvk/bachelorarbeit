/****************************************************************************************
 * Skript:    S4-to-SureTax_GetChangedEntries.groovy
 * Autor:     Senior SAP CPI Developer (Groovy)
 * Zweck:     Holt geänderte Steuerregionen aus CCH Sure Tax und liefert diese
 *            im SAP-Zielformat zurück.
 * Hinweise:  – Das Skript ist modular aufgebaut (siehe Funktionsübersicht).
 *            – Alle relevanten Schritte werden als Attachment geloggt.
 *            – Fehler werden zentral behandelt, Payload wird angehängt.
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import javax.xml.parsers.*
import java.nio.charset.StandardCharsets

/* =========================================================================
 *  Funktionsübersicht
 * -------------------------------------------------------------------------
 *  1. processData               – Haupteinstieg in das Skript
 *  2. getContextValues          – Properties & Header lesen / vorbelegen
 *  3. validateInput             – Pflichtprüfungen
 *  4. buildRequestPayload       – Request-Mapping
 *  5. callSureTaxApi            – API-Call (POST)
 *  6. mapResponsePayload        – Response-Mapping
 *  7. addLogAttachment          – Logging-Helfer
 *  8. handleError               – Zentrales Error-Handling
 * ====================================================================== */

/* =========================================================================
 *  1. Haupteinstieg
 * -------------------------------------------------------------------------
 *  Führt den kompletten End-to-End-Prozess aus.
 * ====================================================================== */
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    def originalBody = message.getBody(String) ?: ''

    try {
        /* 1. Logging eingehender Payload */
        addLogAttachment(messageLog, '01-Incoming_Payload', originalBody, 'text/xml')

        /* 2. Kontextwerte lesen */
        def ctx = getContextValues(message)

        /* 3. Pflichtvalidierung */
        validateInput(ctx)

        /* 4. Request-Mapping */
        String requestBody = buildRequestPayload(ctx)
        addLogAttachment(messageLog, '02-After_Request_Mapping', requestBody, 'text/xml')

        /* 5. API-Aufruf */
        String responseBody = callSureTaxApi(ctx, requestBody, messageLog)
        addLogAttachment(messageLog, '03-Response_Payload', responseBody, 'text/xml')

        /* 6. Response-Mapping */
        String finalBody = mapResponsePayload(ctx, responseBody)
        addLogAttachment(messageLog, '04-After_Response_Mapping', finalBody, 'text/xml')

        /* 7. Setze Response als Body & return */
        message.setBody(finalBody)
        return message

    } catch (Exception e) {
        /* Weitergabe an zentrales Error-Handling */
        handleError(originalBody, e, messageLog)
        return message      // wird nie erreicht, handleError wirft Exception
    }
}

/* =========================================================================
 *  2. Kontextwerte ermitteln
 * -------------------------------------------------------------------------
 *  Liest alle benötigten Header & Properties und setzt Default-Placeholder,
 *  falls nicht vorhanden.
 * ====================================================================== */
private Map getContextValues(Message message) {

    Map ctx = [:]

    /* Header / Properties auslesen oder mit Platzhalter belegen */
    ctx.username            = message.getProperty('sureTaxUsername')    ?: 'placeholder'
    ctx.password            = message.getProperty('sureTaxPassword')    ?: 'placeholder'
    ctx.url                 = message.getProperty('sureTaxURL')         ?: 'placeholder'
    ctx.unifyInd            = message.getProperty('exchangejcdunifyind')?: 'placeholder'

    /* State-Map: entweder aus Property oder Standard initialisieren */
    ctx.stateMap = (message.getProperty('stateMap') instanceof Map)
            ? (Map) message.getProperty('stateMap')
            : ["00":"US",
               "01":"AL",
               "02":"AK",
               "03":"AL",
               "04":"AZ",
               "05":"AR",
               "06":"CA"]

    return ctx
}

/* =========================================================================
 *  3. Pflichtvalidierung
 * -------------------------------------------------------------------------
 *  Prüft, ob die Property exchangejcdunifyind vorhanden und nicht leer ist.
 * ====================================================================== */
private void validateInput(Map ctx) {
    if (!ctx.unifyInd || ctx.unifyInd.trim().isEmpty()) {
        throw new IllegalStateException('Pflichtproperty exchangejcdunifyind fehlt oder ist leer.')
    }
}

/* =========================================================================
 *  4. Request-Mapping erstellen
 * -------------------------------------------------------------------------
 *  Erzeugt den XML-Request für den SureTax Service.
 * ====================================================================== */
private String buildRequestPayload(Map ctx) {

    /* MarkupBuilder erzeugt Ziel-XML */
    def writer = new StringWriter()
    def xml   = new MarkupBuilder(writer)
    xml.'ns3:GetAllGeocodes'('xmlns:ns3':'http://soa.noventic.com/GeocodeService/GeocodeService-V1') {
        'ns3:ClientNumber'(ctx.username)
        'ns3:ValidationKey'(ctx.password)
    }
    return writer.toString()
}

/* =========================================================================
 *  5. API-Aufruf Richtung SureTax
 * -------------------------------------------------------------------------
 *  Führt einen HTTP-POST mit Basic-Auth aus und liefert das Response-XML
 *  als String zurück.
 * ====================================================================== */
private String callSureTaxApi(Map ctx, String requestBody, def messageLog) {

    try {
        def urlConn = new URL(ctx.url).openConnection()
        urlConn.setRequestMethod('POST')
        urlConn.setDoOutput(true)

        /* Basic-Auth Header */
        def basicAuth = "${ctx.username}:${ctx.password}".bytes.encodeBase64().toString()
        urlConn.setRequestProperty('Authorization', "Basic ${basicAuth}")

        /* Request Body schreiben */
        urlConn.getOutputStream().withWriter('UTF-8') { it << requestBody }

        /* Antwort lesen */
        int rc = urlConn.responseCode
        String responseText

        if (rc in 200..299) {
            responseText = urlConn.inputStream.getText('UTF-8')
        } else {
            responseText = urlConn.errorStream?.getText('UTF-8') ?: ''
            throw new RuntimeException("HTTP Fehlercode ${rc} von SureTax.")
        }
        /* ResponseCode als Custom Header loggen */
        messageLog?.setStringProperty('SureTaxHTTPStatus', rc.toString())
        return responseText

    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Aufruf der SureTax API: ${e.message}", e)
    }
}

/* =========================================================================
 *  6. Response-Mapping
 * -------------------------------------------------------------------------
 *  Wandelt das SureTax-Response-XML in das SAP-Zielschema um.
 * ====================================================================== */
private String mapResponsePayload(Map ctx, String sureTaxResponse) {

    def slurper = new XmlSlurper(false,false)
    def respRoot = slurper.parseText(sureTaxResponse)

    /* Alle Geocodes extrahieren                          */
    def geoCodes = respRoot.'**'.findAll { it.name() == 'string' }*.text()

    /* Ziel-XML bauen                                      */
    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)
    xml.'ns2:TAX_JUR_GETCHANGELIST_RECEIVE'('xmlns:ns2':'http://sap.com/xi/FotETaxUS') {
        'ns2:TAX_JURI_CODE' {
            geoCodes.each { String code ->
                def transformed = transformGeocode(code, ctx)
                'ns2:TXJCD'(transformed)
            }
        }
    }
    return writer.toString()
}

/* -------------------------------------------------------------------------
 *  Transformationslogik für einzelne Geocodes
 * --------------------------------------------------------------------- */
private String transformGeocode(String code, Map ctx) {

    if (ctx.unifyInd == 'X') {

        /* Fall 1 – beginnt mit ZZ */
        if (code.startsWith('ZZ')) {
            return 'US' + code
        }

        /* Fall 2 – beginnt mit US */
        if (code.startsWith('US') && code.length() > 4) {
            String stateDigits = code.substring(2,4)            // Zeichen 3-4
            String mappedState = ctx.stateMap[stateDigits] ?: stateDigits
            return code.substring(0,2) + mappedState + code.substring(4) + '-'
        }
    }
    /* Default-Fall */
    return code
}

/* =========================================================================
 *  7. Logging-Helfer
 * -------------------------------------------------------------------------
 *  Hängt Inhalt als String-Attachment ans Message-Log an.
 * ====================================================================== */
private void addLogAttachment(def messageLog, String name, String content, String mime) {
    if (messageLog) {
        messageLog.addAttachmentAsString(name, content ?: '', mime ?: 'text/plain')
    }
}

/* =========================================================================
 *  8. Zentrales Error-Handling
 * -------------------------------------------------------------------------
 *  Fügt Payload als Attachment hinzu und wirft RuntimeException weiter.
 * ====================================================================== */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}