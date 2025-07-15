/****************************************************************************************
 *  Groovy-Script  |  S/4HANA Cloud  ->  CCH Sure Tax – Get Changed Geocodes
 *
 *  Autor:         Senior Integration Developer
 *  Beschreibung:  Holt die geänderten Steuer-Jurisdictions über die Sure Tax-API
 *                 und liefert das Ergebnis im SAP-Zielschema zurück.
 *
 *  Anforderungen: Siehe Aufgabenstellung (Modularität, Logging, Error-Handling, etc.)
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import java.nio.charset.StandardCharsets

// === Haupteinstieg ==============================================================
Message processData(Message message) {

    // MessageLog holen (kann in Tests null sein)
    def messageLog = messageLogFactory?.getMessageLog(message)

    try {
        /* 1) Kontextwerte (Header / Properties) ermitteln & validieren */
        def ctx = setContextValues(message, messageLog)

        /* 2) Request-Mapping ausführen */
        def requestXml = buildRequestPayload(ctx, messageLog)

        /* 3) API-Call */
        def responseXml = callSureTaxAPI(requestXml, ctx, messageLog)

        /* 4) Response-Mapping */
        def mappedResponse = mapResponsePayload(responseXml, ctx, messageLog)

        /* 5) Ergebnis in Message schreiben */
        message.setBody(mappedResponse)
        return message

    } catch (Exception e) {
        // Ursprüngliches Body lesen – falls noch nicht String, konvertieren
        def originalBody = message.getBody(String) ?: ''
        handleError(originalBody, e, messageLog)
        // handleError wirft Exception – Code nachfolgend wird nicht erreicht
    }
}

/* =============================================================================
 *  Modul 1 – Kontextwerte sammeln (Header & Properties) + Validierung
 * ===========================================================================*/
private Map setContextValues(Message message, def messageLog) {

    // Helper zum Lesen von Property → Header → Fallback „placeholder“
    def readCtxValue = { String name ->
        def val = message.getProperty(name) ?: message.getHeader(name, String)
        (val != null && val.toString().trim()) ? val.toString().trim() : 'placeholder'
    }

    Map ctx = [
            username          : readCtxValue('sureTaxUsername'),
            password          : readCtxValue('sureTaxPassword'),
            url               : readCtxValue('sureTaxURL'),
            unifyInd          : readCtxValue('exchangejcdunifyind'),
            stateMap          : message.getProperty('stateMap') instanceof Map
                                ? (Map) message.getProperty('stateMap')
                                : [ : ]                                // leer falls nicht vorhanden
    ]

    // Validierung
    if (ctx.unifyInd == 'placeholder') {
        throw new IllegalStateException('Pflicht-Property exchangejcdunifyind fehlt.')
    }

    return ctx
}

/* =============================================================================
 *  Modul 2 – Request-Mapping (S/4HANA  ->  Sure Tax)
 * ===========================================================================*/
private String buildRequestPayload(Map ctx, def messageLog) {

    // Namespace des Zielschemas
    final String NS3 = 'http://soa.noventic.com/GeocodeService/GeocodeService-V1'

    // XML via MarkupBuilder erzeugen
    def sw = new StringWriter()
    def xml = new groovy.xml.MarkupBuilder(sw)
    xml."ns3:GetAllGeocodes"('xmlns:ns3': NS3) {
        "ns3:ClientNumber"(ctx.username)
        "ns3:ValidationKey"(ctx.password)
    }

    String requestXml = sw.toString()

    // Logging
    logPayload(messageLog, '02_RequestPayload', requestXml)

    return requestXml
}

/* =============================================================================
 *  Modul 3 – HTTP-Aufruf Sure Tax
 * ===========================================================================*/
private String callSureTaxAPI(String requestXml, Map ctx, def messageLog) {

    logPayload(messageLog, '01_InboundPayload', requestXml)   // ursprüngliches Mapping als Referenz

    URL url = new URL(ctx.url)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')

    // Basic-Auth Header setzen
    String auth = "${ctx.username}:${ctx.password}"
    String encAuth = auth.getBytes(StandardCharsets.UTF_8).encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${encAuth}")

    // Request senden
    conn.outputStream.withWriter('UTF-8') { it << requestXml }

    int rc = conn.responseCode
    if (rc != 200) {
        String err = conn.errorStream?.getText('UTF-8') ?: ''
        throw new RuntimeException("Sure Tax-API Response-Code ${rc}. Fehler: ${err}")
    }

    String response = conn.inputStream.getText('UTF-8')
    logPayload(messageLog, '03_ResponsePayload', response)
    return response
}

/* =============================================================================
 *  Modul 4 – Response-Mapping (Sure Tax  ->  SAP-Zielschema)
 * ===========================================================================*/
private String mapResponsePayload(String responseXml, Map ctx, def messageLog) {

    // Response parsen
    def slurper = new XmlSlurper(false, false)
    def resp = slurper.parseText(responseXml)

    // Alle <string>-Nodes einsammeln (Namenraum ignorieren)
    def strings = resp.'**'.findAll { it.name() == 'string' }*.text()

    List<String> txjcdList = []

    strings.each { String s ->
        if (ctx.unifyInd == 'X') {

            if (s.startsWith('ZZ')) {
                txjcdList << 'US' + s
            } else if (s.startsWith('US') && s.length() >= 4) {

                String stateKey = s.substring(2, 4)                     // Position 3-4
                String mappedState = ctx.stateMap[stateKey] ?: stateKey // Mapping oder Original
                String rest = s.substring(4)                            // ab Position 5
                txjcdList << "US${mappedState}${rest}-"
            } else {
                // keine der obigen Bedingungen – unverändert
                txjcdList << s
            }

        } else {
            // unifyInd ungleich 'X' – unverändert
            txjcdList << s
        }
    }

    // Ergebnis-XML bauen
    final String NS2 = 'http://sap.com/xi/FotETaxUS'
    def sw = new StringWriter()
    def xml = new groovy.xml.MarkupBuilder(sw)
    xml."ns2:TAX_JUR_GETCHANGELIST_RECEIVE"('xmlns:ns2': NS2) {
        "ns2:TAX_JURI_CODE" {
            txjcdList.each { code ->
                "ns2:TXJCD"(code)
            }
        }
    }

    String mapped = sw.toString()
    logPayload(messageLog, '04_MappedResponse', mapped)

    return mapped
}

/* =============================================================================
 *  Modul 5 – Logging (Attachment)
 * ===========================================================================*/
private void logPayload(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content, 'text/xml')
}

/* =============================================================================
 *  Modul 6 – Error-Handling (wird überall via try/catch verwendet)
 * ===========================================================================*/
private void handleError(String body, Exception e, def messageLog) {
    // Originalpayload anhängen
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}