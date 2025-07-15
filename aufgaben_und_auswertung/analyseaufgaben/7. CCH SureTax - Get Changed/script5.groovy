/*****************************************************************************
*  SAP Cloud Integration – Groovy-Skript                                     *
*  Aufgabe: “Get Changed” – S/4 HANA CLOUD  →  CCH Sure Tax                 *
*                                                                            *
*  Autor  : ChatGPT (Senior-Developer)                                       *
*  Datum  : 16.06.2025                                                      *
******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets

/* ------------------------------------------------------------------------- *
 *  Haupteinstieg                                                            *
 * ------------------------------------------------------------------------- */
Message processData(Message message) {

    /* ------------------------------------------------------------- *
     *  Initiales Logging-Objekt                                      *
     * ------------------------------------------------------------- */
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* --------------------------------------------------------- *
         *  Header / Property Sicherstellung                         *
         * --------------------------------------------------------- */
        def techValues = ensurePropertiesAndHeaders(message)

        /* --------------------------------------------------------- *
         *  Eingehenden Payload sichern                              *
         * --------------------------------------------------------- */
        def inBody = (message.getBody(String) ?: '')
        logStep(messageLog, 'IncomingPayload', inBody)

        /* --------------------------------------------------------- *
         *  Validierung Pflichtfelder                                *
         * --------------------------------------------------------- */
        validateInput(techValues.exchangejcdunifyind)

        /* --------------------------------------------------------- *
         *  Request-Mapping                                          *
         * --------------------------------------------------------- */
        String requestPayload = buildRequestPayload(techValues)
        logStep(messageLog, 'RequestPayload', requestPayload)

        /* --------------------------------------------------------- *
         *  API Call – Get Changed                                   *
         * --------------------------------------------------------- */
        String responsePayload = callGetChanged(techValues, requestPayload)
        logStep(messageLog, 'ResponsePayload', responsePayload)

        /* --------------------------------------------------------- *
         *  Response-Mapping                                         *
         * --------------------------------------------------------- */
        String mappedResponse = buildResponsePayload(responsePayload, techValues)
        logStep(messageLog, 'MappedResponsePayload', mappedResponse)

        /* --------------------------------------------------------- *
         *  Ergebnis zurück in die Message                           *
         * --------------------------------------------------------- */
        message.setBody(mappedResponse)
        return message

    } catch (Exception e) {
        /* Übergeordnetes Error-Handling                            */
        handleError((message.getBody(String) ?: ''), e, messageLog)
    }
}

/* =========================================================================
 *  Funktion: ensurePropertiesAndHeaders
 * -------------------------------------------------------------------------
 *  Stellt sicher, dass alle benötigten Properties & Header verfügbar sind.
 *  Fehlende Einträge werden mit “placeholder” angelegt.
 * ========================================================================= */
private Map ensurePropertiesAndHeaders(Message message) {
    Map<String, Object> values = [:]

    /* Hilfsclosure zum Ermitteln eines Properties / Headers */
    def fetch = { String key ->
        def val = message.getProperty(key)
        if (val == null) { // Prüfe zusätzlich Header
            val = message.getHeader(key, Object)
        }
        if (val == null) {
            // setze Placeholder, damit nachfolgende Schritte nicht null erhalten
            message.setProperty(key, 'placeholder')
            val = 'placeholder'
        }
        return val
    }

    /* Benötigte Properties ermitteln bzw. anlegen                       */
    values.sureTaxUsername      = fetch('sureTaxUsername')
    values.sureTaxPassword      = fetch('sureTaxPassword')
    values.sureTaxURL           = fetch('sureTaxURL')
    values.exchangejcdunifyind  = fetch('exchangejcdunifyind')

    /* stateMap – wenn nicht vorhanden, Default-Map anlegen              */
    def defaultMap = [
            '00': 'US',
            '01': 'AL', '02': 'AK', '03': 'AL',
            '04': 'AZ', '05': 'AR', '06': 'CA'
    ]
    def sm = message.getProperty('stateMap')
    if (!(sm instanceof Map)) { sm = defaultMap }
    values.stateMap = sm

    return values
}

/* =========================================================================
 *  Funktion: validateInput
 * -------------------------------------------------------------------------
 *  Validiert Pflichtfeld JCD_UNIFY_IND (exchangejcdunifyind).
 * ========================================================================= */
private void validateInput(String unifyInd) {
    if (!unifyInd) {
        throw new IllegalArgumentException('Property exchangejcdunifyind (JCD_UNIFY_IND) fehlt oder ist leer.')
    }
}

/* =========================================================================
 *  Funktion: buildRequestPayload
 * -------------------------------------------------------------------------
 *  Erstellt das SOAP-/XML-Request-Payload gem. Mapping-Spezifikation.
 * ========================================================================= */
private String buildRequestPayload(Map techValues) {

    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)
    xml.'ns3:GetAllGeocodes'('xmlns:ns3': 'http://soa.noventic.com/GeocodeService/GeocodeService-V1') {
        'ns3:ClientNumber'(techValues.sureTaxUsername)
        'ns3:ValidationKey'(techValues.sureTaxPassword)
    }
    return writer.toString()
}

/* =========================================================================
 *  Funktion: callGetChanged
 * -------------------------------------------------------------------------
 *  Führt den HTTP-POST gegen Sure Tax aus und liefert die Antwort zurück.
 *  Bei fehlerhaftem HTTP-Status wird Exception geworfen.
 * ========================================================================= */
private String callGetChanged(Map techValues, String requestBody) {

    URL url = new URL(techValues.sureTaxURL as String)
    HttpURLConnection connection = (HttpURLConnection) url.openConnection()
    connection.requestMethod = 'POST'
    connection.doOutput      = true
    connection.setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')

    /* Basic-Auth Header zusammenbauen                                  */
    String basicAuth = "${techValues.sureTaxUsername}:${techValues.sureTaxPassword}"
    String encoded   = basicAuth.bytes.encodeBase64().toString()
    connection.setRequestProperty('Authorization', "Basic $encoded")

    /* Request-Body senden                                              */
    connection.outputStream.withWriter('UTF-8') { writer ->
        writer << requestBody
    }

    /* Response lesen                                                   */
    int rc = connection.responseCode
    if (rc != 200) {
        String err = connection.errorStream?.getText('UTF-8') ?: ''
        throw new RuntimeException("HTTP-Fehler ${rc} beim Aufruf von Sure Tax. Details: $err")
    }
    return connection.inputStream.getText('UTF-8')
}

/* =========================================================================
 *  Funktion: buildResponsePayload
 * -------------------------------------------------------------------------
 *  Transformiert das Sure-Tax-Response-XML in das Ziel-Schema.
 * ========================================================================= */
private String buildResponsePayload(String responseXml, Map techValues) {

    def response = new XmlSlurper(false, false).parseText(responseXml)
    def entries  = response.'**'.findAll { it.name() == 'string' }

    List<String> txjcdList = []

    entries.each { node ->
        String val = node.text()
        String processed = transformJurisdiction(val, techValues.exchangejcdunifyind as String,
                                                 techValues.stateMap as Map<String,String>)
        txjcdList << processed
    }

    /* Ziel-XML erzeugen                                                */
    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)
    xml.'ns2:TAX_JUR_GETCHANGELIST_RECEIVE'('xmlns:ns2': 'http://sap.com/xi/FotETaxUS') {
        'ns2:TAX_JURI_CODE' {
            txjcdList.each { code ->
                'ns2:TXJCD'(code)
            }
        }
    }
    return writer.toString()
}

/* =========================================================================
 *  Funktion: transformJurisdiction
 * -------------------------------------------------------------------------
 *  Wendet die Transformationsregeln auf einen einzelnen Code an.
 * ========================================================================= */
private String transformJurisdiction(String original, String unifyInd, Map<String,String> stateMap) {

    if (unifyInd != 'X') {            // Keine Besonderbehandlung
        return original
    }

    if (original.startsWith('ZZ')) {   // Regel 1
        return 'US' + original.substring(2)
    }

    if (original.startsWith('US') && original.length() >= 4) {  // Regel 2
        String stateKey = original.substring(2, 4)              // Zeichen 3-4
        String mapped   = stateMap[stateKey] ?: stateKey        // Map oder Fallback
        String rest     = original.substring(2)                 // ab Position 3
        return 'US' + mapped + rest + '-'                       // „-” ans Ende
    }

    /* Fallback – sollte nie eintreten                               */
    return original
}

/* =========================================================================
 *  Funktion: logStep
 * -------------------------------------------------------------------------
 *  Fügt dem MessageLog einen Anhang hinzu (String-Repräsentation).
 * ========================================================================= */
private void logStep(def messageLog, String name, String content) {
    if (messageLog) {
        messageLog.addAttachmentAsString(name, content, 'text/xml')
    }
}

/* =========================================================================
 *  Funktion: handleError
 * -------------------------------------------------------------------------
 *  Zentrales Error-Handling. Hängt den fehlerhaften Payload an und wirft
 *  eine RuntimeException mit eindeutiger Meldung.
 * ========================================================================= */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}