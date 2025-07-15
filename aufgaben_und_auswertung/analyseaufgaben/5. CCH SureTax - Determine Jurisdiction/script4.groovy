/******************************************************************************
 *  Groovy-Skript   :  S4HANA Cloud  ➜  CCH SureTax – Jurisdiction Lookup
 *  Autor           :  Senior Integration Developer
 *
 *  Beschreibung    :
 *  1.   Extrahiert/initialisiert benötigte Properties & Header
 *  2.   Validiert Pflichtfelder (Country / State)
 *  3.   Entscheidet, ob eine Adresse (UseSureAddress) übertragen wird
 *  4.   Erzeugt entsprechendes Request-Payload (2 Mapping-Varianten)
 *  5.   Führt HTTP-POST gegen SureTax API aus
 *  6.   Mapped die Response auf SAP-Zielschema (2 Mapping-Varianten)
 *  7.   Fügt an allen relevanten Stellen Log-Attachments hinzu
 *  8.   Durchgängiges Error-Handling mit Payload-Anreicherung
 ******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.*

// Haupteinstieg der Groovy-Engine
Message processData(Message message) {

    // Initialisierung von MessageLog (falls Monitoring aktiv)
    def messageLog = messageLogFactory?.getMessageLog(message)

    try {
        // 1) Properties & Header ermitteln
        Map props = collectProperties(message)

        // 2) Pflichtfeld-Validierung
        validateInput(props)

        // 3) Entscheidung „UseSureAddress“
        boolean useSureAddress = decideUseSureAddress(props)
        message.setProperty('UseSureAddress', useSureAddress.toString())

        // 4) Request-Payload aufbauen
        String requestXml = buildRequestPayload(props, useSureAddress)

        // Logging: Eingehender Payload & Request
        addLogAttachment(messageLog, '01_InputPayload', message.getBody(String))
        addLogAttachment(messageLog, '02_RequestToSureTax', requestXml)

        // 5) API-Call
        String responseXml = callSureTax(props, useSureAddress, requestXml, messageLog)

        // Logging: Response
        addLogAttachment(messageLog, '03_ResponseSureTax', responseXml)

        // 6) Response-Mapping
        String mappedResponse = mapResponse(props, responseXml, useSureAddress)

        // Logging: Gemapptes Ergebnis
        addLogAttachment(messageLog, '04_MappedResponse', mappedResponse)

        // 7) Ergebnis in Message-Body zurückgeben
        message.setBody(mappedResponse)
        return message

    } catch (Exception e) {
        // Zentrales Error-Handling
        handleError(message.getBody(String), e, messageLog)
    }
}

/* ---------------------------------------------------------------------------
 *  Modul-Funktionen
 * -------------------------------------------------------------------------*/

/** Sammeln & ggf. Initialisieren aller benötigten Properties / Header      */
private Map collectProperties(Message msg) {
    Map<String, String> p = [:]

    // Schlüssel   →   Default („placeholder“, falls nicht vorhanden)
    ['sureTaxUsername',
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
     'exchangecountry'].each { key ->
        def val = (msg.getProperty(key) ?: msg.getHeader(key, String)) ?: 'placeholder'
        p[key] = val.toString().trim()
        msg.setProperty(key, p[key])   // Sicherstellen, dass Property gesetzt ist
    }
    return p
}

/** Pflichtfeld-Prüfung                                                     */
private void validateInput(Map p) {
    if (!p.exchangecountry || p.exchangecountry.equalsIgnoreCase('placeholder')) {
        throw new IllegalArgumentException('Property exchangecountry ist mandatory, aber nicht vorhanden.')
    }
    if (!p.exchangestate  || p.exchangestate.equalsIgnoreCase('placeholder')) {
        throw new IllegalArgumentException('Property exchangestate ist mandatory, aber nicht vorhanden.')
    }
}

/** Entscheidung, ob SureAddress verwendet wird                             */
private boolean decideUseSureAddress(Map p) {

    boolean enableAV   = p.exchangeEnableAV  && !p.exchangeEnableAV.equalsIgnoreCase('placeholder')
    boolean hasStreet  = p.exchangestreet    && !p.exchangestreet.equalsIgnoreCase('placeholder')
    boolean hasState   = p.exchangestate     && !p.exchangestate.equalsIgnoreCase('placeholder')
    boolean hasZipOrCity = (p.exchangezipcode && !p.exchangezipcode.equalsIgnoreCase('placeholder')) ||
                           (p.exchangecity    && !p.exchangecity.equalsIgnoreCase('placeholder'))

    return enableAV && hasStreet && hasState && hasZipOrCity
}

/** Erzeugt Request XML nach gewähltem Mapping                              */
private String buildRequestPayload(Map p, boolean useSureAddress) {

    // Hilfsfunktion: Zipcode gem. Regel
    Closure zipTransform = { String zip, String country ->
        if (['US', 'USA'].contains(country?.toUpperCase())) {
            return zip
        }
        return zip?.tokenize('-')?.first() ?: ''
    }

    if (useSureAddress) {
        // ------------------ Request „WithAddress“ -------------------------
        def writer = new StringWriter()
        def mb = new MarkupBuilder(writer)

        mb.'ns4:SoapRequest'('xmlns:ns4':'http://sureaddress.net/') {
            'ns4:request' {
                'ns4:ClientNumber'(p.sureTaxUsername)
                'ns4:PrimaryAddressLine'(p.exchangestreet)
                'ns4:SecondaryAddressLine'(p.exchangestreet1?.equalsIgnoreCase('placeholder') ? '' : p.exchangestreet1)
                'ns4:ZIPCode'( zipTransform(p.exchangezipcode, p.exchangecountry) )
                'ns4:ResponseType'('S')
            }
        }
        return writer.toString()

    } else {
        // ------------------ Request „WithoutAddress“ ----------------------
        def writer = new StringWriter()
        def mb = new MarkupBuilder(writer)

        mb.'soapenv:Envelope'('xmlns:soapenv':'http://schemas.xmlsoap.org/soap/envelope/',
                              'xmlns:ns1':'http://service.example.com/xsd') {
            'soapenv:Header'()
            'soapenv:Body' {
                'ns1:GetGeocodeByZipcodeOrAddress' {
                    'ns1:ClientNumber'(p.sureTaxUsername)
                    'ns1:DataYear'( Calendar.getInstance().get(Calendar.YEAR) )
                    'ns1:Zipcode'( zipTransform(p.exchangezipcode, p.exchangecountry) )
                    'ns1:CountryCode'(p.exchangecountry)
                    'ns1:City'( p.exchangecity?.equalsIgnoreCase('placeholder') ? '' : p.exchangecity )
                }
            }
        }
        return writer.toString()
    }
}

/** HTTP-Aufruf an SureTax-Service                                          */
private String callSureTax(Map p, boolean useSureAddress, String body, def messageLog) {

    // URL zusammensetzen
    String suffix = useSureAddress ? '/useAddress' : '/useTax'
    String urlStr = "${p.sureTaxURL}${suffix}"

    URL url = new URL(urlStr)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')

    // Basic-Auth Header
    String auth = "${p.sureTaxUsername}:${p.sureTaxPassword}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${auth}")

    // Request-Body senden
    conn.outputStream.withWriter('UTF-8') { it << body }

    int rc = conn.responseCode
    if (rc != 200) {
        addLogAttachment(messageLog, 'HTTPErrorResponse', conn.errorStream?.getText('UTF-8') ?: '')
        throw new RuntimeException("SureTax API call failed - HTTP RC=${rc}")
    }

    // Response lesen
    return conn.inputStream.getText('UTF-8')
}

/** Mapped SureTax-Response ➜ SAP-Zielschema                               */
private String mapResponse(Map p, String responseXml, boolean useSureAddress) {

    def slurper = new XmlSlurper(false, false)   // no validating / no namespace awareness

    if (useSureAddress) {
        // ------------------ Mapping „WithAddress“ ------------------------
        def resp = slurper.parseText(responseXml)
        def ns0  = new groovy.xml.Namespace('http://sureaddress.net/', 'ns0')

        String stateResp   = (resp[ns0.SoapRequestResult][ns0.State].text())
        String geocodeResp = (resp[ns0.SoapRequestResult][ns0.GeoCode].text())
        String streetResp  = (resp[ns0.SoapRequestResult][ns0.PrimaryAddressLine].text())

        String txjcd = buildTxjcd(geocodeResp, stateResp ?: p.exchangestate, p.exchangejcdunifyind)

        def writer = new StringWriter()
        def mb = new MarkupBuilder(writer)
        mb.'ns2:TAX_JURISDICTION_RECEIVE'('xmlns:ns2':'http://sap.com/xi/FotETaxUS') {
            'LOCATION_HEADER' { 'RETCODE'('0') }
            'LOCATION_RESULTS' {
                'COUNTRY'(p.exchangecountry)
                'STATE'(stateResp)
                'TXJCD'(txjcd)
                'STREET_ADDRESS' { 'STREET'(streetResp) }
            }
        }
        return writer.toString()

    } else {
        // ------------------ Mapping „WithoutAddress“ ---------------------
        def env    = slurper.parseText(responseXml)
        def ns1    = new groovy.xml.Namespace('http://service.example.com/xsd', 'ns1')
        String geocodeResp = env.'**'.find { it.name() == ns1.Geocode.name() }?.text()
        String stateCode   = env.'**'.find { it.name() == ns1.StateCode.name() }?.text()

        String txjcd = buildTxjcd(geocodeResp, stateCode, p.exchangejcdunifyind)

        def writer = new StringWriter()
        def mb = new MarkupBuilder(writer)
        mb.'ns2:TAX_JURISDICTION_RECEIVE'('xmlns:ns2':'http://sap.com/xi/FotETaxUS') {
            'LOCATION_HEADER' { 'RETCODE'('0') }
            'LOCATION_RESULTS' {
                'COUNTRY'(p.exchangecountry)
                'STATE'(p.exchangestate)
                'ZIPCODE'(p.exchangezipcode)
                'TXJCD'(txjcd)
            }
        }
        return writer.toString()
    }
}

/** Erzeugt TXJCD gem. JCD_UNIFY_IND                                       */
private String buildTxjcd(String geocode, String state, String unifyInd) {
    if (!geocode) { return '' }

    if (unifyInd?.equalsIgnoreCase('X')) {
        String first2 = geocode.length() >= 2 ? geocode[0..1] : geocode
        String rest   = geocode.length() > 2  ? geocode[2..-1] : ''
        return "${first2}${state}${rest}-"
    }
    return geocode
}

/** Fügt dem MessageLog einen String als Attachment hinzu                   */
private void addLogAttachment(def messageLog, String name, String content) {
    // CPI erlaubt null-Safety: messageLog kann bei deaktiviertem Trace null sein
    messageLog?.addAttachmentAsString(name, content ?: '', 'text/plain')
}

/** Zentrales Error-Handling                                                */
private void handleError(String body, Exception e, def messageLog) {
    // Ursprünglichen Payload anhängen
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im SureTax-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}