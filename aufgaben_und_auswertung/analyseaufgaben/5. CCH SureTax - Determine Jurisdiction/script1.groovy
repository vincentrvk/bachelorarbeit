/****************************************************************************************************************
 *  Groovy-Skript zur Ermittlung der Jurisdiction mittels CCH Sure Tax                                           *
 *  Autor: AI-Assistant                                                                                         *
 ****************************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import groovy.xml.MarkupBuilder
import java.net.HttpURLConnection
import java.net.URL

/* -----------------------------------------------------------------------------------------------------------------
 *  Fehlerbehandlung – bei Exception wird der eingehende Payload als Attachment angehängt und die Exception geworfen
 * -----------------------------------------------------------------------------------------------------------------*/
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    throw new RuntimeException("Fehler im Groovy-Skript: ${e.message}", e)
}

/* -----------------------------------------------------------------------------------------------------------------
 *  Haupt-Entry-Point
 * -----------------------------------------------------------------------------------------------------------------*/
Message processData(Message message) {
    def messageLog = messageLogFactory?.getMessageLog(message)
    def inboundPayload = message.getBody(String) ?: ''
    logAttachment(messageLog, "1-Eingehender-Payload", inboundPayload)

    try {
        // Eingehenden XML-Payload einlesen
        def xmlIn = new XmlSlurper().parseText(inboundPayload)
        xmlIn.declareNamespace(['n0':'http://sap.com/xi/FotETaxUS'])

        // Properties sammeln bzw. setzen
        def ctx = collectContextValues(message, xmlIn)

        // Pflichtfelder prüfen
        validateMandatory(ctx)

        // Adresse nutzen? URL-Suffix festlegen
        decideAddressUsage(message, ctx)

        // Request-Payload erstellen
        def requestPayload = ctx.UseSureAddress == 'true' ?
                buildRequestWithAddress(ctx) :
                buildRequestWithoutAddress(ctx)
        logAttachment(messageLog, "2-Request-Payload", requestPayload)

        // HTTP-Call ausführen
        def responsePayload = callSureTax(message, ctx, requestPayload)
        logAttachment(messageLog, "3-Response-Payload", responsePayload)

        // Antwort mappen
        def mappedResponse = ctx.UseSureAddress == 'true' ?
                mapResponseWithAddress(ctx, responsePayload) :
                mapResponseWithoutAddress(ctx, responsePayload)
        logAttachment(messageLog, "4-Mapped-Response", mappedResponse)

        message.setBody(mappedResponse)
        return message
    } catch (Exception e) {
        handleError(inboundPayload, e, messageLog)
    }
}

/* -----------------------------------------------------------------------------------------------------------------
 *  Kontext-Werte sammeln (Properties aus Message oder XML)
 * -----------------------------------------------------------------------------------------------------------------*/
def Map collectContextValues(Message message, def xml) {
    def props = [:]
    def keys = ['sureTaxUsername', 'sureTaxPassword', 'sureTaxURL',
                'exchangejcdunifyind', 'exchangeEnableAV', 'exchangestreet1', 'exchangestreet',
                'exchangeoutofcity', 'exchangestate', 'exchangecounty', 'exchangecity',
                'exchangezipcode', 'exchangecountry']

    def extractor = { String key ->
        switch (key) {
            case 'exchangejcdunifyind': return xml.'n0:LOCATION_DATA'.'n0:JCD_UNIFY_IND'.text()
            case 'exchangeEnableAV'   : return xml.'n0:LOCATION_DATA'.'n0:ENABLE_AV'.text()
            case 'exchangestreet1'    : return xml.'n0:LOCATION_DATA'.'n0:STREET_ADDRESS'.'n0:STREET1'.text()
            case 'exchangestreet'     : return xml.'n0:LOCATION_DATA'.'n0:STREET_ADDRESS'.'n0:STREET'.text()
            case 'exchangeoutofcity'  : return xml.'n0:LOCATION_DATA'.'n0:OUTOF_CITY'.text()
            case 'exchangestate'      : return xml.'n0:LOCATION_DATA'.'n0:STATE'.text()
            case 'exchangecounty'     : return xml.'n0:LOCATION_DATA'.'n0:COUNTY'.text()
            case 'exchangecity'       : return xml.'n0:LOCATION_DATA'.'n0:CITY'.text()
            case 'exchangezipcode'    : return xml.'n0:LOCATION_DATA'.'n0:ZIPCODE'.text()
            case 'exchangecountry'    : return xml.'n0:LOCATION_DATA'.'n0:COUNTRY'.text()
            default                   : return null
        }
    }

    keys.each { k ->
        def v = message.getProperty(k) ?: extractor(k)
        props[k] = (v && v.toString().trim()) ? v.toString().trim() : 'placeholder'
        message.setProperty(k, props[k])
    }
    return props
}

/* -----------------------------------------------------------------------------------------------------------------
 *  Pflichtfeld-Validierung
 * -----------------------------------------------------------------------------------------------------------------*/
def validateMandatory(Map ctx) {
    if (ctx.exchangecountry == 'placeholder') {
        throw new IllegalStateException("Pflichtfeld COUNTRY fehlt.")
    }
    if (ctx.exchangestate == 'placeholder') {
        throw new IllegalStateException("Pflichtfeld STATE fehlt.")
    }
}

/* -----------------------------------------------------------------------------------------------------------------
 *  Entscheidung über Adress-Verwendung & URL-Suffix
 * -----------------------------------------------------------------------------------------------------------------*/
def decideAddressUsage(Message message, Map ctx) {
    def useAddress = 'false'
    if (ctx.exchangeEnableAV != 'placeholder' &&
        ctx.exchangestreet  != 'placeholder' &&
        ctx.exchangestate   != 'placeholder' &&
        (ctx.exchangezipcode != 'placeholder' || ctx.exchangecity != 'placeholder')) {
        useAddress = 'true'
    }
    ctx.UseSureAddress = useAddress
    message.setProperty('UseSureAddress', useAddress)

    def suffix = useAddress == 'true' ? '/useAddress' : '/useTax'
    def baseUrl = ctx.sureTaxURL?.replaceAll('/\$', '') ?: 'placeholder'
    ctx.sureTaxURLFinal = baseUrl + suffix
    message.setProperty('sureTaxURLFinal', ctx.sureTaxURLFinal)
}

/* -----------------------------------------------------------------------------------------------------------------
 *  Request-Mapping ohne Adresse
 * -----------------------------------------------------------------------------------------------------------------*/
def String buildRequestWithoutAddress(Map ctx) {
    def year = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy"))
    def w = new StringWriter()
    new MarkupBuilder(w).'soapenv:Envelope'(
            'xmlns:soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
            'xmlns:ns1'    : 'http://service.example.com/xsd') {
        'soapenv:Header'()
        'soapenv:Body' {
            'ns1:GetGeocodeByZipcodeOrAddress' {
                'ns1:ClientNumber'(ctx.sureTaxUsername)
                'ns1:DataYear'(year)
                'ns1:Zipcode'(determineZip(ctx))
                'ns1:CountryCode'(ctx.exchangecountry)
                'ns1:City'(ctx.exchangecity == 'placeholder' ? '' : ctx.exchangecity)
            }
        }
    }
    w.toString()
}

/* -----------------------------------------------------------------------------------------------------------------
 *  Request-Mapping mit Adresse
 * -----------------------------------------------------------------------------------------------------------------*/
def String buildRequestWithAddress(Map ctx) {
    def w = new StringWriter()
    new MarkupBuilder(w).'ns4:SoapRequest'('xmlns:ns4': 'http://sureaddress.net/') {
        'ns4:request' {
            'ns4:ClientNumber'(ctx.sureTaxUsername)
            'ns4:PrimaryAddressLine'(ctx.exchangestreet)
            'ns4:SecondaryAddressLine'(ctx.exchangestreet1 == 'placeholder' ? '' : ctx.exchangestreet1)
            'ns4:ZIPCode'(determineZip(ctx))
            'ns4:ResponseType'('S')
        }
    }
    w.toString()
}

/* -----------------------------------------------------------------------------------------------------------------
 *  ZIP-Ermittlung abhängig vom Land
 * -----------------------------------------------------------------------------------------------------------------*/
def String determineZip(Map ctx) {
    if (ctx.exchangecountry in ['USA', 'US']) {
        return ctx.exchangezipcode
    }
    ctx.exchangezipcode?.tokenize('-')?.first()
}

/* -----------------------------------------------------------------------------------------------------------------
 *  HTTP-POST gegen Sure Tax
 * -----------------------------------------------------------------------------------------------------------------*/
def String callSureTax(Message message, Map ctx, String payload) {
    if (ctx.sureTaxURLFinal == 'placeholder') {
        throw new IllegalStateException("sureTaxURL nicht definiert.")
    }
    HttpURLConnection conn = (HttpURLConnection) new URL(ctx.sureTaxURLFinal).openConnection()
    conn.with {
        requestMethod = 'POST'
        doOutput      = true
        setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')
        setRequestProperty('Authorization',
                "Basic ${("${ctx.sureTaxUsername}:${ctx.sureTaxPassword}").bytes.encodeBase64().toString()}")
        outputStream.with { it.write(payload.getBytes('UTF-8')) }
    }
    def resp
    conn.with {
        if (responseCode in 200..299) {
            resp = inputStream.getText('UTF-8')
        } else {
            def err = errorStream ? errorStream.getText('UTF-8') : ''
            throw new RuntimeException("HTTP-Fehler ${responseCode}: ${err}")
        }
    }
    resp
}

/* -----------------------------------------------------------------------------------------------------------------
 *  Response-Mapping ohne Adresse
 * -----------------------------------------------------------------------------------------------------------------*/
def String mapResponseWithoutAddress(Map ctx, String resp) {
    def env = new XmlSlurper().parseText(resp)
    env.declareNamespace(['soapenv':'http://schemas.xmlsoap.org/soap/envelope/',
                          'ns1'    :'http://service.example.com/xsd'])
    def geo = env.'soapenv:Body'.'ns1:GetGeocodeByZipcodeOrAddressResponse'.'ns1:Geocode'.text()
    def stateCode = env.'soapenv:Body'.'ns1:GetGeocodeByZipcodeOrAddressResponse'.'ns1:StateCode'.text()
    def txjcd = buildTxjcd(ctx, geo, stateCode)

    def w = new StringWriter()
    new MarkupBuilder(w).'ns2:TAX_JURISDICTION_RECEIVE'('xmlns:ns2':'http://sap.com/xi/FotETaxUS') {
        'LOCATION_HEADER' { 'RETCODE'('0') }
        'LOCATION_RESULTS' {
            'COUNTRY'(ctx.exchangecountry)
            'STATE'(ctx.exchangestate)
            'ZIPCODE'(ctx.exchangezipcode)
            'TXJCD'(txjcd)
        }
    }
    w.toString()
}

/* -----------------------------------------------------------------------------------------------------------------
 *  Response-Mapping mit Adresse
 * -----------------------------------------------------------------------------------------------------------------*/
def String mapResponseWithAddress(Map ctx, String resp) {
    def root = new XmlSlurper().parseText(resp)
    root.declareNamespace(['ns0':'http://sureaddress.net/'])
    def state  = root.'ns0:SoapRequestResult'.'ns0:State'.text()
    def geo    = root.'ns0:SoapRequestResult'.'ns0:GeoCode'.text()
    def street = root.'ns0:SoapRequestResult'.'ns0:PrimaryAddressLine'.text()
    def txjcd  = buildTxjcd(ctx, geo, state)

    def w = new StringWriter()
    new MarkupBuilder(w).'ns2:TAX_JURISDICTION_RECEIVE'('xmlns:ns2':'http://sap.com/xi/FotETaxUS') {
        'LOCATION_HEADER' { 'RETCODE'('0') }
        'LOCATION_RESULTS' {
            'COUNTRY'(ctx.exchangecountry)
            'STATE'(state)
            'TXJCD'(txjcd)
            'STREET_ADDRESS' { 'STREET'(street) }
        }
    }
    w.toString()
}

/* -----------------------------------------------------------------------------------------------------------------
 *  TXJCD-Erzeugung abhängig von JCD_UNIFY_IND
 * -----------------------------------------------------------------------------------------------------------------*/
def String buildTxjcd(Map ctx, String geocode, String stateCode) {
    if (ctx.exchangejcdunifyind?.equalsIgnoreCase('X') && geocode.length() >= 2) {
        return geocode.substring(0, 2) + stateCode + geocode.substring(2) + "-"
    }
    geocode
}

/* -----------------------------------------------------------------------------------------------------------------
 *  Attachment-Logging
 * -----------------------------------------------------------------------------------------------------------------*/
def void logAttachment(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content, "text/xml")
}