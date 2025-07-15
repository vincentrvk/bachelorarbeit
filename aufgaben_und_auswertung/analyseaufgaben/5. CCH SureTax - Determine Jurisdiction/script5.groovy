/****************************************************************************************************************
 *  SAP Cloud Integration – Groovy-Skript
 *
 *  Zweck  : Ermittlung der Steuer-Jurisdiction via CCH Sure Tax
 *  Autor  : ChatGPT – Senior Integration Developer
 *  Hinweis: Skript ist modular aufgebaut, erfüllt sämtliche in der Aufgabenstellung definierten Anforderungen.
 ***************************************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.text.SimpleDateFormat
import java.net.HttpURLConnection
import java.util.Base64

/* =======================================================  Haupt-Einstieg  ================================= */
Message processData(Message message) {

    /* -------- Vorbereitungen ------------------------------------------------------ */
    def originalBody      = message.getBody(String)                                  // Eingehender Payload
    def messageLog        = messageLogFactory.getMessageLog(message)                 // CPI-MessageLog

    /* -------- Verarbeitung mit Error-Handling ------------------------------------- */
    try {

        addLogAttachment(messageLog, "1_IncomingPayload", originalBody)

        /* 1. Properties & Header lesen / vorbelegen */
        def props = prepareInitialValues(message)

        /* 2. Validierung der Muss-Felder */
        validateInput(props)

        /* 3. Entscheidung, ob Adresse verwendet wird */
        determineAddressUsage(props, message)

        /* 4. Request-Mapping */
        String requestXml = (props.UseSureAddress == "true") ?
                            buildRequestWithAddress(props) :
                            buildRequestWithoutAddress(props)

        addLogAttachment(messageLog, "2_RequestMapping", requestXml)

        /* 5. API-Call  */
        String responseXml = callSureTax(requestXml, props, messageLog)

        addLogAttachment(messageLog, "3_ResponsePayload", responseXml)

        /* 6. Response-Mapping */
        String finalResponse = (props.UseSureAddress == "true") ?
                               mapResponseWithAddress(responseXml, props) :
                               mapResponseWithoutAddress(responseXml, props)

        addLogAttachment(messageLog, "4_ResponseMapping", finalResponse)

        /* 7. Message-Body auf Ergebnis setzen und zurückgeben */
        message.setBody(finalResponse)
        return message
    }
    catch (Exception e) {
        handleError(originalBody, e, messageLog)     // Delegation an zentrales Error-Handling
    }
}

/* ==========================================  Modul: Initiale Property-Ermittlung  ========================== */
/*  Liest alle benötigten Properties/Headers aus dem Message-Objekt.                                            */
Map<String, String> prepareInitialValues(Message msg) {

    Map<String, String> props = [:]

    /*--- Liste der erwarteten Properties (siehe Aufgabenstellung) ------------------------------------------ */
    [
        'sureTaxUsername', 'sureTaxPassword', 'sureTaxURL',
        'exchangejcdunifyind',
        'exchangeEnableAV', 'exchangestreet1', 'exchangestreet',
        'exchangeoutofcity', 'exchangestate', 'exchangecounty',
        'exchangecity', 'exchangezipcode', 'exchangecountry'
    ].each { key ->
        def val    = msg.getProperty(key)
        props[key] = (val == null || val.toString().trim().isEmpty()) ? "placeholder" : val.toString().trim()
    }

    return props
}

/* ===============================================  Modul: Validierung  ====================================== */
/*  Prüft Vorhandensein aller Muss-Felder und wirft RuntimeException bei Verletzung.                          */
void validateInput(Map props) {

    if (!props.exchangecountry || props.exchangecountry == "placeholder")
        throw new IllegalArgumentException("Pflichtfeld COUNTRY fehlt oder ist leer.")
    if (!props.exchangestate   || props.exchangestate   == "placeholder")
        throw new IllegalArgumentException("Pflichtfeld STATE fehlt oder ist leer.")
}

/* ====================================  Modul: Entscheidung Adressverwendung  =============================== */
void determineAddressUsage(Map props, Message msg) {

    boolean useAddress = (
            props.exchangeEnableAV &&
            props.exchangeEnableAV != "placeholder" &&
            props.exchangeEnableAV.trim()
        ) &&
        (props.exchangestreet  && props.exchangestreet  != "placeholder") &&
        (props.exchangestate   && props.exchangestate   != "placeholder") &&
        (
            (props.exchangezipcode && props.exchangezipcode != "placeholder") ||
            (props.exchangecity    && props.exchangecity    != "placeholder")
        )

    props.UseSureAddress = useAddress.toString()
    props.urlSuffix      = useAddress ? "/useAddress" : "/useTax"

    /* Property in Message ablegen, damit nachfolgende Steps (falls vorhanden) darauf zugreifen können */
    msg.setProperty("UseSureAddress", props.UseSureAddress)
    msg.setProperty("urlSuffix",      props.urlSuffix)
}

/* ==============================================  Modul: Request-Mapping  ==================================== */
String buildRequestWithoutAddress(Map props) {

    StringWriter sw = new StringWriter()
    def mb = new MarkupBuilder(sw)
    mb.'soapenv:Envelope'('xmlns:soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
                         'xmlns:ns1'    : 'http://service.example.com/xsd') {
        'soapenv:Header'()
        'soapenv:Body' {
            'ns1:GetGeocodeByZipcodeOrAddress' {
                'ns1:ClientNumber'  props.sureTaxUsername
                'ns1:DataYear'      new SimpleDateFormat("yyyy").format(new Date())
                'ns1:Zipcode'       deriveZip(props.exchangezipcode, props.exchangecountry)
                'ns1:CountryCode'   props.exchangecountry
                'ns1:City'          props.exchangecity ?: ''
            }
        }
    }
    return sw.toString()
}

String buildRequestWithAddress(Map props) {

    StringWriter sw = new StringWriter()
    def mb = new MarkupBuilder(sw)
    mb.'ns4:SoapRequest'('xmlns:ns4': 'http://sureaddress.net/') {
        'ns4:request' {
            'ns4:ClientNumber'          props.sureTaxUsername
            'ns4:PrimaryAddressLine'    props.exchangestreet
            'ns4:SecondaryAddressLine'  (props.exchangestreet1 ?: '')
            'ns4:ZIPCode'               deriveZip(props.exchangezipcode, props.exchangecountry)
            'ns4:ResponseType'          'S'
        }
    }
    return sw.toString()
}

/* ================================================  Modul: API-Call  ========================================= */
String callSureTax(String requestXml, Map props, def messageLog) {

    String url = props.sureTaxURL + props.urlSuffix
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", "text/xml; charset=UTF-8")

    /* ---- Basic-Auth ------------------------------------------------------- */
    String auth = "${props.sureTaxUsername}:${props.sureTaxPassword}"
    String enc  = Base64.encoder.encodeToString(auth.bytes)
    conn.setRequestProperty("Authorization", "Basic ${enc}")

    /* ---- Request senden --------------------------------------------------- */
    conn.outputStream.withWriter("UTF-8") { it << requestXml }
    int rc = conn.responseCode
    String response = conn.inputStream.getText("UTF-8")

    messageLog?.addAttachmentAsString("HTTP_Status", rc.toString(), "text/plain")

    if (rc != 200) {
        throw new RuntimeException("HTTP-Request an Sure Tax schlug fehl (Status ${rc}).")
    }
    return response
}

/* ==============================================  Modul: Response-Mapping  =================================== */
String mapResponseWithoutAddress(String response, Map props) {

    def xmlResp = new XmlSlurper().parseText(response)

    /* Elemente suchen – unabhängig vom Namespace */
    def geocodeNode   = xmlResp.'**'.find { it.name().toString().endsWith('Geocode') }
    def stateCodeNode = xmlResp.'**'.find { it.name().toString().endsWith('StateCode') }

    String geocode   = geocodeNode?.text()   ?: ''
    String stateCode = stateCodeNode?.text() ?: ''

    String txjcd     = buildTxjcd(geocode, stateCode, props.exchangejcdunifyind)

    /* ------ Ausgabe-XML erzeugen ---------------------------------------- */
    StringWriter sw = new StringWriter()
    def mb = new MarkupBuilder(sw)
    mb.'ns2:TAX_JURISDICTION_RECEIVE'('xmlns:ns2': 'http://sap.com/xi/FotETaxUS') {
        'LOCATION_HEADER' {
            'RETCODE'('0')
        }
        'LOCATION_RESULTS' {
            'COUNTRY'(props.exchangecountry)
            'STATE'  (props.exchangestate)
            'ZIPCODE'(props.exchangezipcode)
            'TXJCD'  (txjcd)
        }
    }
    return sw.toString()
}

String mapResponseWithAddress(String response, Map props) {

    def xmlResp  = new XmlSlurper().parseText(response)

    def stateNode   = xmlResp.'**'.find { it.name().toString().endsWith('State') }
    def geoNode     = xmlResp.'**'.find { it.name().toString().endsWith('GeoCode') }
    def streetNode  = xmlResp.'**'.find { it.name().toString().endsWith('PrimaryAddressLine') }

    String geocode    = geoNode?.text()    ?: ''
    String stateCode  = stateNode?.text()  ?: ''
    String streetLine = streetNode?.text() ?: ''

    String txjcd = buildTxjcd(geocode, stateCode, props.exchangejcdunifyind)

    /* ------ Ausgabe-XML erzeugen ---------------------------------------- */
    StringWriter sw = new StringWriter()
    def mb = new MarkupBuilder(sw)
    mb.'ns2:TAX_JURISDICTION_RECEIVE'('xmlns:ns2': 'http://sap.com/xi/FotETaxUS') {
        'LOCATION_HEADER' {
            'RETCODE'('0')
        }
        'LOCATION_RESULTS' {
            'COUNTRY'(props.exchangecountry)
            'STATE'  (stateCode)
            'TXJCD'  (txjcd)
            'STREET_ADDRESS' {
                'STREET'(streetLine)
            }
        }
    }
    return sw.toString()
}

/* ====================================  Hilfs-Methoden  ===================================================== */

/* Liefert ZIP nach Transformationsregel */
String deriveZip(String zip, String country) {
    if (!zip) return ''
    if (["USA", "US"].contains(country?.toUpperCase())) {
        return zip
    }
    return zip.split('-')[0]
}

/* Berechnet TXJCD gem. Unify-Indikator */
String buildTxjcd(String geocode, String stateCode, String unifyInd) {

    if (!geocode) return ''
    if ("X".equalsIgnoreCase(unifyInd)) {
        if (geocode.length() < 3) return geocode   // Sicherheitsnetz
        return geocode.substring(0,2) + stateCode + geocode.substring(2) + "-"
    }
    return geocode
}

/* Fügt beliebigen String-Attachment im CPI-Monitor hinzu */
void addLogAttachment(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content ?: '', "text/xml")
}

/* =================================================  Zentrales Error-Handling  ============================== */
void handleError(String body, Exception e, def messageLog) {

    /* Payload als Attachment anhängen */
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: '', "text/xml")

    String errMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}