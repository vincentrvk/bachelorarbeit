/*****************************************************************************************
*  SAP CPI – Groovy-Skript
*  Bestimmung der Jurisdiction und Aufbau des Request/Response-Mappings
*
*  Autor: (auto-generated)
*****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.time.LocalDate
import java.time.format.DateTimeFormatter

Message processData(Message message) {

    /*--------------------------------------------------
      1. Logging des Ursprungs-Payloads
    --------------------------------------------------*/
    def msgLog     = messageLogFactory?.getMessageLog(message)
    def original   = message.getBody(String) as String
    logAttachment(msgLog, '01_OriginalPayload', original)

    try {

        /*--------------------------------------------------
          2. Initiale Property-Befüllung & Validierung
        --------------------------------------------------*/
        setInitialProperties(message, original)
        validateMandatoryProperties(message)

        /*--------------------------------------------------
          3. Entscheidung Adress-Nutzung & URL-Suffix
        --------------------------------------------------*/
        decideAddressUsage(message)

        /*--------------------------------------------------
          4. Mapping (Request oder Response)
        --------------------------------------------------*/
        def currentBody = message.getBody(String) as String
        if (isTaxSendPayload(currentBody)) {
            /* Request-Mapping */
            def requestXml = buildRequest(message)
            message.setBody(requestXml)
            logAttachment(msgLog, '02_RequestMapping', requestXml)
        } else {
            /* Response-Mapping */
            logAttachment(msgLog, '03_ResponsePayload', currentBody)
            def responseXml = buildResponse(message, currentBody)
            message.setBody(responseXml)
            logAttachment(msgLog, '04_ResponseMapping', responseXml)
        }

    } catch (Exception e) {
        /*--------------------------------------------------
          5. Zentrales Error-Handling
        --------------------------------------------------*/
        handleError(original, e, msgLog)
    }

    return message
}

/* =============================================================================
   Modul 3 – Header / Property Handling
============================================================================= */
def setInitialProperties(Message message, String xmlBody) {

    def slurper = new XmlSlurper().parseText(xmlBody)
    def n0      = new groovy.xml.Namespace('http://sap.com/xi/FotETaxUS', 'n0')

    /* Hilfs-Closure: Property nur setzen, wenn nicht vorhanden */
    def ensure = { key, value ->
        if (message.getProperty(key) == null)
            message.setProperty(key, value ?: 'placeholder')
    }

    ensure('exchangejcdunifyind',  slurper[n0.TAX_JURISDICTION_SEND]?.LOCATION_DATA?.JCD_UNIFY_IND?.text())
    ensure('exchangestreet1',      slurper[n0.TAX_JURISDICTION_SEND]?.LOCATION_DATA?.STREET_ADDRESS?.STREET1?.text())
    ensure('exchangestreet',       slurper[n0.TAX_JURISDICTION_SEND]?.LOCATION_DATA?.STREET_ADDRESS?.STREET?.text())
    ensure('exchangeoutofcity',    slurper[n0.TAX_JURISDICTION_SEND]?.LOCATION_DATA?.OUTOF_CITY?.text())
    ensure('exchangestate',        slurper[n0.TAX_JURISDICTION_SEND]?.LOCATION_DATA?.STATE?.text())
    ensure('exchangecounty',       slurper[n0.TAX_JURISDICTION_SEND]?.LOCATION_DATA?.COUNTY?.text())
    ensure('exchangecity',         slurper[n0.TAX_JURISDICTION_SEND]?.LOCATION_DATA?.CITY?.text())
    ensure('exchangezipcode',      slurper[n0.TAX_JURISDICTION_SEND]?.LOCATION_DATA?.ZIPCODE?.text())
    ensure('exchangecountry',      slurper[n0.TAX_JURISDICTION_SEND]?.LOCATION_DATA?.COUNTRY?.text())

    /* fallback für externe Credentials / URL */
    ensure('sureTaxUsername', message.getProperty('sureTaxUsername'))
    ensure('sureTaxPassword', message.getProperty('sureTaxPassword'))
    ensure('sureTaxURL',      message.getProperty('sureTaxURL'))
    ensure('exchangeEnableAV',message.getProperty('exchangeEnableAV'))
}

/* =============================================================================
   Modul Validierung
============================================================================= */
def validateMandatoryProperties(Message message) {
    if (!message.getProperty('exchangecountry') ||
        message.getProperty('exchangecountry') == 'placeholder')
        throw new RuntimeException('Pflichtfeld COUNTRY fehlt.')

    if (!message.getProperty('exchangestate')   ||
        message.getProperty('exchangestate') == 'placeholder')
        throw new RuntimeException('Pflichtfeld STATE fehlt.')
}

/* =============================================================================
   Modul Entscheidung Adress-Nutzung
============================================================================= */
def decideAddressUsage(Message message) {

    def useAddress =  message.getProperty('exchangeEnableAV') &&
                      message.getProperty('exchangeEnableAV') != 'placeholder' &&
                      message.getProperty('exchangestreet')   &&
                      message.getProperty('exchangestate')    &&
                      (message.getProperty('exchangezipcode') ||
                       message.getProperty('exchangecity'))

    message.setProperty('UseSureAddress', useAddress.toString())

    def baseUrl = (message.getProperty('sureTaxURL') ?: '')
    message.setProperty('sureTaxURL', baseUrl + (useAddress ? '/useAddress' : '/useTax'))
}

/* =============================================================================
   Modul 4 – Request-Mapping
============================================================================= */
def buildRequest(Message message) {
    message.getProperty('UseSureAddress') == 'true' ?
            buildRequestWithAddress(message) :
            buildRequestWithoutAddress(message)
}

/* --- Request ohne Adresse ----------------------------------------------- */
def buildRequestWithoutAddress(Message message) {

    def writer = new StringWriter()
    new MarkupBuilder(writer).'soapenv:Envelope'(
            'xmlns:soapenv':'http://schemas.xmlsoap.org/soap/envelope/',
            'xmlns:ns1'    :'http://service.example.com/xsd') {
        'soapenv:Header'()
        'soapenv:Body' {
            'ns1:GetGeocodeByZipcodeOrAddress' {
                'ns1:ClientNumber' (message.getProperty('sureTaxUsername'))
                'ns1:DataYear'     (LocalDate.now().format(DateTimeFormatter.ofPattern('yyyy')))
                'ns1:Zipcode'      (computeZip(message))
                'ns1:CountryCode'  (message.getProperty('exchangecountry'))
                'ns1:City'         (message.getProperty('exchangecity') ?: '')
            }
        }
    }
    writer.toString()
}

/* --- Request mit Adresse ------------------------------------------------- */
def buildRequestWithAddress(Message message) {

    def writer = new StringWriter()
    new MarkupBuilder(writer).'ns4:SoapRequest'('xmlns:ns4':'http://sureaddress.net/') {
        'ns4:request' {
            'ns4:ClientNumber'        (message.getProperty('sureTaxUsername'))
            'ns4:PrimaryAddressLine'  (message.getProperty('exchangestreet'))
            'ns4:SecondaryAddressLine'(message.getProperty('exchangestreet1') ?: '')
            'ns4:ZIPCode'             (computeZip(message))
            'ns4:ResponseType'        ('S')
        }
    }
    writer.toString()
}

/* =============================================================================
   Modul 4 – Response-Mapping
============================================================================= */
def buildResponse(Message message, String responseBody) {

    responseBody.contains('GetGeocodeByZipcodeOrAddressResponse') ?
            buildResponseWithoutAddress(message, responseBody) :
            buildResponseWithAddress(message,  responseBody)
}

/* --- Response ohne Adresse ---------------------------------------------- */
def buildResponseWithoutAddress(Message message, String responseBody) {

    def slurper   = new XmlSlurper().parseText(responseBody)
    def geocode   = slurper.'**'.find{it.name()=='Geocode'}?.text()
    def stateCode = slurper.'**'.find{it.name()=='StateCode'}?.text()

    responseTemplate {
        'LOCATION_RESULTS' {
            'COUNTRY' (message.getProperty('exchangecountry'))
            'STATE'   (message.getProperty('exchangestate'))
            'ZIPCODE' (message.getProperty('exchangezipcode'))
            'TXJCD'   (computeTxjcd(geocode, stateCode, message))
        }
    }
}

/* --- Response mit Adresse ------------------------------------------------ */
def buildResponseWithAddress(Message message, String responseBody) {

    def slurper  = new XmlSlurper().parseText(responseBody)
    def state    = slurper.'**'.find{it.name()=='State'}?.text()
    def geocode  = slurper.'**'.find{it.name()=='GeoCode'}?.text()
    def street   = slurper.'**'.find{it.name()=='PrimaryAddressLine'}?.text()

    responseTemplate {
        'LOCATION_RESULTS' {
            'COUNTRY'(message.getProperty('exchangecountry'))
            'STATE'  (state)
            'TXJCD'  (computeTxjcd(geocode, state, message))
            'STREET_ADDRESS' {
                'STREET'(street)
            }
        }
    }
}

/* --- Gemeinsamer Response-Stamm ----------------------------------------- */
def responseTemplate(Closure bodyClosure) {
    def writer = new StringWriter()
    new MarkupBuilder(writer).'ns2:TAX_JURISDICTION_RECEIVE'('xmlns:ns2':'http://sap.com/xi/FotETaxUS') {
        'LOCATION_HEADER' { 'RETCODE' ('0') }
        bodyClosure.delegate = delegate
        bodyClosure()
    }
    writer.toString()
}

/* =============================================================================
   Modul Hilfs-Funktionen
============================================================================= */
/* ZIP-Berechnung gem. Regeln */
def computeZip(Message message) {
    def zip     = message.getProperty('exchangezipcode') ?: ''
    def country = message.getProperty('exchangecountry')?.toUpperCase()
    country in ['US','USA'] ? zip : zip.tokenize('-')[0]
}

/* TXJCD-Berechnung gem. Regeln */
def computeTxjcd(String geocode, String stateCode, Message message) {
    if (!geocode) return ''
    message.getProperty('exchangejcdunifyind')?.equalsIgnoreCase('X') ?
            "${geocode.substring(0,2)}${stateCode}${geocode.substring(2)}-" :
            geocode
}

/* Ursprungs-Payload = TAX_JURISDICTION_SEND? */
def isTaxSendPayload(String body) {
    body?.contains('TAX_JURISDICTION_SEND')
}

/* Logging als Message-Attachment */
def logAttachment(def msgLog, String name, String content) {
    msgLog?.addAttachmentAsString(name, content ?: '', 'text/xml')
}

/* =============================================================================
   Modul Error-Handling
============================================================================= */
def handleError(String body, Exception e, def msgLog) {
    msgLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    throw new RuntimeException("Fehler im Mapping-Skript: ${e.message}", e)
}