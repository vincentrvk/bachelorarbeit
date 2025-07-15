/****************************************************************************************
 *  Groovy-Skript  :   GetS4JurisdictionToSureTax.groovy
 *  Beschreibung  :   Ermittelt anhand der eingehenden Adresse den Zuständigkeitsbereich
 *                    (Jurisdiction) über den Sure-Tax Service.
 *
 *  Autor         :   AI – Senior Integration Developer
 *  Version       :   1.0
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import java.text.SimpleDateFormat
import java.net.HttpURLConnection
import java.net.URL
import groovy.xml.MarkupBuilder
import groovy.xml.StreamingMarkupBuilder

/* ======================================================================================
   >>>  Einstiegspunkt
   ====================================================================================== */
Message processData(Message message) {

    /* ------------------------------------------------------------------
       Vorbereitung
     ------------------------------------------------------------------ */
    def msgLog = messageLogFactory.getMessageLog(message)
    def incomingBody = message.getBody(String) ?: ''
    logAttachment(msgLog, '01-IncomingPayload', incomingBody)

    try {

        /* ---------------------------------------------
           1. Properties / Header aufbereiten & prüfen
        --------------------------------------------- */
        prepareContext(message)
        validateMandatory(message)

        /* ---------------------------------------------
           2. Adress-Entscheidung treffen
        --------------------------------------------- */
        boolean useSureAddress = determineAddressUsage(message)
        message.setProperty('UseSureAddress', useSureAddress.toString())

        /* ---------------------------------------------
           3. Request-Mapping erstellen
        --------------------------------------------- */
        String requestXml = useSureAddress ?
                buildRequestWithAddress(message) :
                buildRequestWithoutAddress(message)

        logAttachment(msgLog, '02-RequestPayload', requestXml)

        /* ---------------------------------------------
           4. HTTP-Aufruf an Sure-Tax
        --------------------------------------------- */
        String responseXml = callSureTaxAPI(message, requestXml)
        logAttachment(msgLog, '03-ResponsePayload', responseXml)

        /* ---------------------------------------------
           5. Response-Mapping erstellen
        --------------------------------------------- */
        String mappedResponse = useSureAddress ?
                mapResponseWithAddress(message, responseXml) :
                mapResponseWithoutAddress(message, responseXml)

        logAttachment(msgLog, '04-MappedResponse', mappedResponse)

        /* ---------------------------------------------
           6. Ergebnis zurückgeben
        --------------------------------------------- */
        message.setBody(mappedResponse)
        return message

    } catch (Exception ex) {
        handleError(incomingBody, ex, msgLog)   // wirft RuntimeException
    }
}

/* ======================================================================================
   Hilfs-Funktionen
   ====================================================================================== */

/* ------------------------------------------------------------------
 * Bereitet alle benötigten Properties & Header auf.
 * Fehlende Werte werden mit 'placeholder' initialisiert.
 * ------------------------------------------------------------------ */
def prepareContext(Message msg) {

    /* Liste der relevanten Properties gem. Aufgabenstellung                */
    def propertyNames = [
            'sureTaxUsername',
            'sureTaxPassword',
            'sureTaxURL',
            'exchangejcdunifyind',
            'exchangeEnableAV',
            'exchangestreet1',
            'exchangestreet',
            'exchangeoutofcity',
            'exchangestate',
            'exchangecounty',
            'exchangecity',
            'exchangezipcode',
            'exchangecountry'
    ]

    propertyNames.each { pName ->
        def val = msg.getProperty(pName)
        if (val == null || (val instanceof String && val.trim().isEmpty())) {
            msg.setProperty(pName, 'placeholder')
        }
    }
}

/* ------------------------------------------------------------------
 * Pflichtfeld-Validierung
 * ------------------------------------------------------------------ */
def validateMandatory(Message msg) {
    def country = msg.getProperty('exchangecountry') as String
    def state   = msg.getProperty('exchangestate')   as String

    if (country in [null, 'placeholder', '']) {
        throw new IllegalArgumentException('Pflichtfeld COUNTRY fehlt oder ist leer')
    }
    if (state in [null, 'placeholder', '']) {
        throw new IllegalArgumentException('Pflichtfeld STATE fehlt oder ist leer')
    }
}

/* ------------------------------------------------------------------
 * Bestimmt anhand der Regeln, ob die Sure-Address Validierung genutzt
 * werden soll.
 * ------------------------------------------------------------------ */
boolean determineAddressUsage(Message msg) {

    String enableAv = (msg.getProperty('exchangeEnableAV') ?: '').trim()
    String street   = (msg.getProperty('exchangestreet')    ?: '').trim()
    String state    = (msg.getProperty('exchangestate')     ?: '').trim()
    String zip      = (msg.getProperty('exchangezipcode')   ?: '').trim()
    String city     = (msg.getProperty('exchangecity')      ?: '').trim()

    boolean criteria =
            enableAv &&
            street &&
            state &&
            (zip || city)

    /* URL-Suffix gem. Aufgabenstellung                                     */
    msg.setProperty('urlSuffix', criteria ? '/useAddress' : '/useTax')
    return criteria
}

/* ------------------------------------------------------------------
 * Erstellt den SOAP-Request (ohne Adresse)
 * ------------------------------------------------------------------ */
String buildRequestWithoutAddress(Message msg) {

    String clientNumber = msg.getProperty('sureTaxUsername')
    String country      = msg.getProperty('exchangecountry')
    String zip          = msg.getProperty('exchangezipcode') ?: ''
    String city         = msg.getProperty('exchangecity')    ?: ''

    /* Jahreszahl im Format yyyy                                             */
    String year = new SimpleDateFormat('yyyy').format(new Date())

    /* Zipcode-Transformation                                                */
    String zipOut = (country.equalsIgnoreCase('USA') || country.equalsIgnoreCase('US')) ?
            zip :
            (zip.contains('-') ? zip.split('-')[0] : zip)

    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)

    xml.'soapenv:Envelope'(
            'xmlns:soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
            'xmlns:ns1'    : 'http://service.example.com/xsd'
    ) {
        'soapenv:Header'()
        'soapenv:Body'() {
            'ns1:GetGeocodeByZipcodeOrAddress'() {
                'ns1:ClientNumber'(clientNumber)
                'ns1:DataYear'    (year)
                'ns1:Zipcode'     (zipOut)
                'ns1:CountryCode' (country)
                'ns1:City'        (city ?: '')
            }
        }
    }
    return writer.toString()
}

/* ------------------------------------------------------------------
 * Erstellt den Request (mit Adresse)
 * ------------------------------------------------------------------ */
String buildRequestWithAddress(Message msg) {

    String clientNumber = msg.getProperty('sureTaxUsername')
    String street       = msg.getProperty('exchangestreet')
    String street1      = msg.getProperty('exchangestreet1') ?: ''
    String country      = msg.getProperty('exchangecountry')
    String zip          = msg.getProperty('exchangezipcode') ?: ''

    /* Zipcode-Transformation                                                */
    String zipOut = (country.equalsIgnoreCase('USA') || country.equalsIgnoreCase('US')) ?
            zip :
            (zip.contains('-') ? zip.split('-')[0] : zip)

    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)

    xml.'ns4:SoapRequest'('xmlns:ns4': 'http://sureaddress.net/') {
        'ns4:request'() {
            'ns4:ClientNumber'        (clientNumber)
            'ns4:PrimaryAddressLine'  (street)
            'ns4:SecondaryAddressLine'(street1 ?: '')
            'ns4:ZIPCode'             (zipOut)
            'ns4:ResponseType'        ('S')
        }
    }
    return writer.toString()
}

/* ------------------------------------------------------------------
 * Führt den HTTP-POST gegen Sure-Tax aus und liefert die Response.
 * ------------------------------------------------------------------ */
String callSureTaxAPI(Message msg, String requestBody) {

    String baseUrl   = msg.getProperty('sureTaxURL')
    String suffix    = msg.getProperty('urlSuffix')      ?: ''
    String user      = msg.getProperty('sureTaxUsername')
    String password  = msg.getProperty('sureTaxPassword')

    String auth      = "${user}:${password}".bytes.encodeBase64().toString()
    URL url          = new URL(baseUrl + suffix)

    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')
    conn.setRequestProperty('Authorization', "Basic ${auth}")

    conn.outputStream.withWriter('UTF-8') { it << requestBody }

    int rc = conn.responseCode
    InputStream is = (rc >= 200 && rc < 300) ? conn.inputStream : conn.errorStream
    String response = is?.getText('UTF-8') ?: ''

    if (rc < 200 || rc >= 300) {
        throw new RuntimeException("Sure-Tax Service antwortete mit HTTP ${rc}")
    }
    return response
}

/* ------------------------------------------------------------------
 * Mappt die Antwort (ohne Adresse) in das Ziel-Schema
 * ------------------------------------------------------------------ */
String mapResponseWithoutAddress(Message msg, String resp) {

    def env = new XmlSlurper().parseText(resp)
    def ns1 = new groovy.xml.Namespace('http://service.example.com/xsd', 'ns1')

    String geocode   = env.'**'.find { it.name() == ns1.Geocode.name() }?.text()
    String stateCode = env.'**'.find { it.name() == ns1.StateCode.name() }?.text()

    String txjcd = buildTxjcd(geocode, stateCode, msg.getProperty('exchangejcdunifyind'))

    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)

    xml.'ns2:TAX_JURISDICTION_RECEIVE'('xmlns:ns2': 'http://sap.com/xi/FotETaxUS') {
        'LOCATION_HEADER' { 'RETCODE'('0') }
        'LOCATION_RESULTS' {
            'COUNTRY' (msg.getProperty('exchangecountry'))
            'STATE'   (msg.getProperty('exchangestate'))
            'ZIPCODE' (msg.getProperty('exchangezipcode'))
            'TXJCD'   (txjcd)
        }
    }
    return writer.toString()
}

/* ------------------------------------------------------------------
 * Mappt die Antwort (mit Adresse) in das Ziel-Schema
 * ------------------------------------------------------------------ */
String mapResponseWithAddress(Message msg, String resp) {

    def root = new XmlSlurper().parseText(resp)
    def ns0  = new groovy.xml.Namespace('http://sureaddress.net/', 'ns0')

    def resultNode  = root.'**'.find { it.name() == ns0.SoapRequestResult.name() }
    String geocode  = resultNode?.'ns0:GeoCode'?.text()
    String state    = resultNode?.'ns0:State'?.text()
    String street   = resultNode?.'ns0:PrimaryAddressLine'?.text()

    String txjcd = buildTxjcd(geocode, state, msg.getProperty('exchangejcdunifyind'))

    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)

    xml.'ns2:TAX_JURISDICTION_RECEIVE'('xmlns:ns2': 'http://sap.com/xi/FotETaxUS') {
        'LOCATION_HEADER' { 'RETCODE'('0') }
        'LOCATION_RESULTS' {
            'COUNTRY' (msg.getProperty('exchangecountry'))
            'STATE'   (state)
            'TXJCD'   (txjcd)
            'STREET_ADDRESS' {
                'STREET'(street)
            }
        }
    }
    return writer.toString()
}

/* ------------------------------------------------------------------
 * Baut den TXJCD String gem. Unify-Regel auf.
 * ------------------------------------------------------------------ */
String buildTxjcd(String geocode, String stateCode, String unifyInd) {

    if (!geocode) {
        return ''
    }

    if (unifyInd?.equalsIgnoreCase('X') && stateCode) {
        String first2   = geocode.substring(0, 2)
        String rest     = geocode.substring(2)
        return "${first2}${stateCode}${rest}-"
    }
    return geocode
}

/* ------------------------------------------------------------------
 * Fügt Inhalte als Message-Attachment hinzu.
 * ------------------------------------------------------------------ */
void logAttachment(def msgLog, String name, String content) {
    msgLog?.addAttachmentAsString(name, content ?: '', 'text/xml')
}

/* ------------------------------------------------------------------
 * Zentrales Error-Handling gem. Vorgabe.
 * ------------------------------------------------------------------ */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}