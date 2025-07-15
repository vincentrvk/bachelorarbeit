/***************************************************************************
*  Groovy-Skript für die Integration S/4HANA Cloud  ->  CCH SureTax
*  Autor  :  ChatGPT (Senior-Integrator)
*  Zweck  :  – Request- / Response-Mapping
*           – HTTP-Call an SureTax
*           – Validierung, Logging, Error-Handling
****************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import groovy.xml.XmlSlurper
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.net.URL

/****************************************************************************
*  Haupt-Einstiegspunkt der Groovy-Ausführung
****************************************************************************/
Message processData(Message message) {

    // MessageLog holen
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /*------------------------------------------------------------------
        * 1. Eingehenden Payload sichern
        *-----------------------------------------------------------------*/
        final String incomingPayload = message.getBody(String) ?: ''
        addLogAttachment(messageLog, '01-IncomingPayload', incomingPayload)

        /*------------------------------------------------------------------
        * 2. Header / Properties einlesen
        *-----------------------------------------------------------------*/
        def ctx = initContext(message)

        /*------------------------------------------------------------------
        * 3. Pflichtfeld prüfen
        *-----------------------------------------------------------------*/
        if (!ctx.exchangejcdunifyind) {
            throw new IllegalArgumentException("Pflicht-Property 'exchangejcdunifyind' fehlt oder ist leer.")
        }

        /*------------------------------------------------------------------
        * 4. Request-Mapping erstellen
        *-----------------------------------------------------------------*/
        final String requestXml = buildRequestPayload(ctx)
        addLogAttachment(messageLog, '02-RequestMapping', requestXml)

        /*------------------------------------------------------------------
        * 5. HTTP-Call an SureTax
        *-----------------------------------------------------------------*/
        final String responseXml = callSureTax(ctx, requestXml)
        addLogAttachment(messageLog, '03-ResponsePayload', responseXml)

        /*------------------------------------------------------------------
        * 6. Response-Mapping
        *-----------------------------------------------------------------*/
        final String mappedResponse = mapResponsePayload(responseXml, ctx)
        addLogAttachment(messageLog, '04-ResponseMapping', mappedResponse)

        /*------------------------------------------------------------------
        * 7. Rückgabe in Message-Body
        *-----------------------------------------------------------------*/
        message.setBody(mappedResponse)
        return message
    }
    catch (Exception e) {
        handleError(message.getBody(String) ?: '', e, messageLog)
        return message     // wird nie erreicht, aber notwendig für Compiler
    }
}

/****************************************************************************
*  Liest Header & Properties  –  setzt Platzhalter, falls nicht vorhanden
****************************************************************************/
private Map initContext(Message message) {
    return [
            sureTaxUsername      : message.getProperty('sureTaxUsername')      ?: 'placeholder',
            sureTaxPassword      : message.getProperty('sureTaxPassword')      ?: 'placeholder',
            sureTaxURL           : message.getProperty('sureTaxURL')           ?: 'placeholder',
            exchangejcdunifyind  : message.getProperty('exchangejcdunifyind')  ?: 'placeholder',
            // State-Map: entweder als Property mitgegeben oder Default-Werte nutzen
            stateMap             : (message.getProperty('stateMap') instanceof Map)
                                    ? message.getProperty('stateMap')
                                    : ["00":"US","01":"AL","02":"AK","03":"AL","04":"AZ","05":"AR","06":"CA"]
    ]
}

/****************************************************************************
*  Erstellt das Request-XML für SureTax
****************************************************************************/
private String buildRequestPayload(Map ctx) {
    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)
    xml.'ns3:GetAllGeocodes'('xmlns:ns3': 'http://soa.noventic.com/GeocodeService/GeocodeService-V1') {
        'ns3:ClientNumber'(ctx.sureTaxUsername)
        'ns3:ValidationKey'(ctx.sureTaxPassword)
    }
    return sw.toString()
}

/****************************************************************************
*  Führt den HTTP-POST-Call an SureTax aus
****************************************************************************/
private String callSureTax(Map ctx, String requestBody) {

    URL url = new URL(ctx.sureTaxURL)
    HttpURLConnection con = (HttpURLConnection) url.openConnection()
    con.with {
        requestMethod = 'POST'
        setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')
        // Basic-Auth Header zusammenbauen
        def userPass = "${ctx.sureTaxUsername}:${ctx.sureTaxPassword}".bytes.encodeBase64().toString()
        setRequestProperty('Authorization', "Basic ${userPass}")
        doOutput = true
        outputStream.withWriter(StandardCharsets.UTF_8.name()) { it << requestBody }
    }

    // HTTP-Status prüfen
    int httpCode = con.responseCode
    if (httpCode != HttpURLConnection.HTTP_OK) {
        throw new RuntimeException("HTTP-Fehler beim Aufruf von SureTax. Status: ${httpCode}")
    }

    // Antwort lesen
    return con.inputStream.getText(StandardCharsets.UTF_8.name())
}

/****************************************************************************
*  Mappt die Response auf das Ziel-Schema
****************************************************************************/
private String mapResponsePayload(String responseBody, Map ctx) {

    // XML einlesen (Namespace wird ignoriert, da Prefix unbekannt sein kann)
    def response = new XmlSlurper(false, false).parseText(responseBody)
    // Alle <string>-Nodes einsammeln
    def codes = response.'**'.findAll { it.name() == 'string' }*.text()

    // Transformation lt. Business-Regeln
    List<String> transformed = codes.collect { code ->
        transformCode(code, ctx.exchangejcdunifyind as String, ctx.stateMap as Map)
    }

    // Ziel-XML erzeugen
    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)
    xml.'ns2:TAX_JUR_GETCHANGELIST_RECEIVE'('xmlns:ns2': 'http://sap.com/xi/FotETaxUS') {
        'ns2:TAX_JURI_CODE' {
            transformed.each { val ->
                'ns2:TXJCD'(val)
            }
        }
    }
    return sw.toString()
}

/****************************************************************************
*  Wandelt einen einzelnen Geocode gemäß den Transformations-Regeln um
****************************************************************************/
private String transformCode(String code, String unifyInd, Map stateMap) {

    if (unifyInd == 'X') {

        if (code.startsWith('ZZ')) {
            // Regel: ZZ  ->  US + Rest
            return 'US' + code.substring(2)

        } else if (code.startsWith('US') && code.length() >= 4) {
            // Regel: US-Codes modifizieren
            String stateNumeric = code.substring(2, 4)          // Zeichen 3-4
            String stateAbbr    = stateMap[stateNumeric] ?: stateNumeric
            String rest         = code.substring(4)
            return 'US' + stateAbbr + rest + '-'
        }
    }
    // Default-Fall
    return code
}

/****************************************************************************
*  Fügt Payloads als Message-Attachment hinzu
****************************************************************************/
private void addLogAttachment(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content ?: '', 'text/xml')
}

/****************************************************************************
*  Einheitliches Error-Handling
****************************************************************************/
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}