/*****************************************************************************************
 *  SAP Cloud Integration – Groovy-Skript
 *  Geocode – S/4HANA Cloud  ►  CCH Sure Tax
 *
 *  Aufgaben:
 *   • Properties & Header ermitteln
 *   • Eingehende Nachricht validieren
 *   • Request-Mapping  (BatchGetGeocode – Sure-Tax)
 *   • HTTP-POST gegen Sure-Tax (Basic-Auth)
 *   • Response-Mapping  (TAX_JUR_REDEFINE_RECEIVE)
 *   • Logging & Error-Handling
 *
 *  Autor:  AI-Assistant (Senior-Developer)
 *****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import java.net.HttpURLConnection
import java.net.URL
import groovy.xml.*

/* ================================================================================ *
 *                               HAUPTRUTINE                                       *
 * ================================================================================ */
Message processData(Message message) {

    /* ------------------------------------------------------------- *
     * 1. Initialisierung & Logging-Objekt
     * ------------------------------------------------------------- */
    def messageLog = messageLogFactory?.getMessageLog(message)
    def inBody     = message.getBody(String) ?: ''
    logAttachment(messageLog, '01-InboundPayload', inBody)

    try {

        /* ---------------------------------------------------------- *
         * 2. Header/Property-Werte lesen bzw. setzen
         * ---------------------------------------------------------- */
        def cfg = readConfiguration(message, inBody)

        /* ---------------------------------------------------------- *
         * 3. Eingehende XML parsen & validieren
         * ---------------------------------------------------------- */
        def locations = parseAndValidateInbound(inBody)

        /* ---------------------------------------------------------- *
         * 4. Request-XML aufbauen
         * ---------------------------------------------------------- */
        def requestXml = buildRequestXml(cfg.username, locations)
        logAttachment(messageLog, '02-RequestPayload', requestXml)

        /* ---------------------------------------------------------- *
         * 5. HTTP-Call Richtung Sure-Tax
         * ---------------------------------------------------------- */
        def responseXml = callSureTax(cfg.url, cfg.username, cfg.password, requestXml)
        logAttachment(messageLog, '03-ResponsePayload', responseXml)

        /* ---------------------------------------------------------- *
         * 6. Response-Mapping
         * ---------------------------------------------------------- */
        def outXml = buildResponseXml(responseXml, cfg.exchangeInd)
        logAttachment(messageLog, '04-ResponseMapping', outXml)

        /* ---------------------------------------------------------- *
         * 7. Ergebnis zurück an iFlow
         * ---------------------------------------------------------- */
        message.setBody(outXml)
        return message

    } catch (Exception e) {
        /* ---------------------------------------------------------- *
         * 8. Fehlerfall
         * ---------------------------------------------------------- */
        handleError(inBody, e, messageLog)            // => RuntimeException
        return null                                    // (unreachable)
    }
}

/* ================================================================================ *
 *                            HILFSFUNKTIONEN                                       *
 * ================================================================================ */

/* ------------------------------------------------------------------------------
 * Lese Properties / Header oder setze Placeholder.
 * ------------------------------------------------------------------------------ */
private Map readConfiguration(Message msg, String body) {

    def props = msg.getProperties()

    String usr = props?.sureTaxUsername    ?: 'placeholder'
    String pwd = props?.sureTaxPassword    ?: 'placeholder'
    String url = props?.sureTaxURL         ?: 'placeholder'

    // exchangejcdunifyind ggf. aus Payload auslesen, falls nicht als Property vorhanden
    String exchInd = props?.exchangejcdunifyind
    if (!exchInd) {
        def slurper = new XmlSlurper().parseText(body)
        exchInd = slurper.depthFirst().find {
            it.name() == 'JCD_UNIFY_IND'
        }?.text() ?: ''
    }

    return [username: usr, password: pwd, url: url, exchangeInd: exchInd?.trim()]
}

/* ------------------------------------------------------------------------------
 * Eingangs-XML validieren und Locations ermitteln
 * ------------------------------------------------------------------------------ */
private List<Map> parseAndValidateInbound(String xmlString) {

    def inbound = new XmlSlurper().parseText(xmlString)
    def nsFotet = new groovy.xml.Namespace('http://sap.com/xi/FotETaxUS', '')

    List<Map> locList = []

    inbound[nsFotet.LOCATION_SIMPLE].each { loc ->
        def country = (loc.COUNTRY.text() ?: '').trim()
        if (!country) {
            throw new IllegalArgumentException('Validierung fehlgeschlagen: Feld COUNTRY ist leer.')
        }

        def seq  = (loc.SEQUENCE_NUM.text() ?: '').trim()
        def zip  = (loc.ZIPCODE.text()      ?: '').trim()
        def str  = (loc.STREET_ADDRESS_SIMPLE.STREET.text() ?: '').trim()

        locList << [sequence: seq, zipcode: zip, country: country, street: str]
    }
    return locList
}

/* ------------------------------------------------------------------------------
 * Request-XML für Sure-Tax erzeugen
 * ------------------------------------------------------------------------------ */
private String buildRequestXml(String clientNumber, List<Map> locs) {

    def writer = new StringWriter()
    def builder = new MarkupBuilder(writer)
    builder.setDoubleQuotes(true)

    def ns3 = 'http://soa.noventic.com/GeocodeService/GeocodeService-V1'
    def yearStr = new Date().format('yyyy', TimeZone.getTimeZone('UTC'))

    builder.'ns3:BatchGetGeocode'('xmlns:ns3': ns3) {
        'ns3:request' {
            'ns3:ClientNumber'(clientNumber)
            'ns3:DataYear'(yearStr)
            'ns3:LocationList' {
                locs.eachWithIndex { l, idx ->
                    'ns3:Location' {
                        'ns3:SequenceNum'(l.sequence)
                        'ns3:Zipcode'(calculateZip(l.zipcode, l.country))
                        'ns3:AddressLine1'(l.street)
                    }
                }
            }
        }
    }
    return writer.toString()
}

/* ------------------------------------------------------------------------------
 * Zip-Berechnung gemäß Land
 * ------------------------------------------------------------------------------ */
private String calculateZip(String zip, String country) {

    if (country.equalsIgnoreCase('US') || country.equalsIgnoreCase('USA')) {
        return zip.trim()
    }
    // anderenfalls: split an '-' und 1. Teil zurück
    return zip.tokenize('-').first().trim()
}

/* ------------------------------------------------------------------------------
 * Ausführung des HTTP-POST gegen Sure-Tax
 * ------------------------------------------------------------------------------ */
private String callSureTax(String urlStr, String user, String pwd, String body) {

    HttpURLConnection con = (HttpURLConnection) new URL(urlStr).openConnection()
    con.setRequestMethod('POST')
    con.setConnectTimeout(15000)
    con.setReadTimeout(30000)
    con.setDoOutput(true)
    con.setRequestProperty('Content-Type', 'application/xml')
    String auth = "${user}:${pwd}".bytes.encodeBase64().toString()
    con.setRequestProperty('Authorization', "Basic ${auth}")

    con.outputStream.withWriter('UTF-8') { it << body }
    int rc = con.responseCode
    if (rc != HttpURLConnection.HTTP_OK) {
        String err = con.errorStream?.getText('UTF-8') ?: ''
        throw new RuntimeException("Sure-Tax HTTP Fehler ${rc}: ${err}")
    }
    return con.inputStream.getText('UTF-8')
}

/* ------------------------------------------------------------------------------
 * Response-Mapping  ►  TAX_JUR_REDEFINE_RECEIVE
 * ------------------------------------------------------------------------------ */
private String buildResponseXml(String response, String exchangeInd) {

    def ns0 = new groovy.xml.Namespace('http://soa.noventic.com/GeocodeService/GeocodeService-V1', 'ns0')
    def respParsed = new XmlSlurper().parseText(response)

    // State-Map (FIPS  ►  US-State-Code)
    def stateMap = [
        '01':'AL','02':'AK','04':'AZ','05':'AR','06':'CA','08':'CO','09':'CT','10':'DE','11':'DC',
        '12':'FL','13':'GA','15':'HI','16':'ID','17':'IL','18':'IN','19':'IA','20':'KS','21':'KY',
        '22':'LA','23':'ME','24':'MD','25':'MA','26':'MI','27':'MN','28':'MS','29':'MO','30':'MT',
        '31':'NE','32':'NV','33':'NH','34':'NJ','35':'NM','36':'NY','37':'NC','38':'ND','39':'OH',
        '40':'OK','41':'OR','42':'PA','44':'RI','45':'SC','46':'SD','47':'TN','48':'TX','49':'UT',
        '50':'VT','51':'VA','53':'WA','54':'WV','55':'WI','56':'WY'
    ]

    List<Map> list = []
    respParsed[ns0.BatchGetGeocodeResult][ns0.GeocodeResponseList][ns0.GeocodeResponse].each { gr ->
        def geo  = (gr.Geocode.text()     ?: '').trim()
        def seq  = (gr.SequenceNum.text() ?: '').trim()
        def code = (gr.ResponseCode.text()?: '').trim()
        def msg  = (gr.ErrorMessage.text()?: '').trim()

        list << [
            txjcd   : transformTxjcd(geo, exchangeInd, stateMap),
            seq     : seq,
            retcode : (code == '9999') ? '0'    : '1',
            errcode : (code == '9999') ? '0000' : '1999',
            errmsg  : msg
        ]
    }

    /* ------------------- XML erzeugen ------------------- */
    def writer = new StringWriter()
    def builder = new MarkupBuilder(writer)
    builder.setDoubleQuotes(true)

    builder.'ns2:TAX_JUR_REDEFINE_RECEIVE'('xmlns:ns2':'http://sap.com/xi/FotETaxUS') {
        list.each { m ->
            'ns2:TAX_JURI_CODE_NUM' {
                'ns2:TXJCD'(m.txjcd)
                'ns2:SEQUENCE_NUM'(m.seq)
                'ns2:MSG_RETURN'{
                    'ns2:RETCODE'(m.retcode)
                    'ns2:ERRCODE'(m.errcode)
                    'ns2:ERRMSG' (m.errmsg)
                }
            }
        }
    }
    return writer.toString()
}

/* ------------------------------------------------------------------------------
 * TXJCD Transformation
 * ------------------------------------------------------------------------------ */
private String transformTxjcd(String geocode, String ind, Map stateMap) {

    if (ind == 'X') {
        if (geocode.startsWith('ZZ')) {
            return 'US' + geocode
        }
        if (geocode.startsWith('US') && geocode.length() > 4) {
            def stCode = geocode.substring(2,4)
            def mapped = stateMap.get(stCode, stCode)
            return geocode[0..1] + mapped + geocode.substring(4) + '-'
        }
    }
    return geocode
}

/* ------------------------------------------------------------------------------
 * Logging – fügt ein Attachment im MessageLog hinzu
 * ------------------------------------------------------------------------------ */
private void logAttachment(def log, String name, String content) {
    log?.addAttachmentAsString(name, content, 'text/xml')
}

/* ------------------------------------------------------------------------------
 * Zentrales Error-Handling  (wirft RuntimeException)
 * ------------------------------------------------------------------------------ */
private void handleError(String body, Exception e, def msgLog) {
    msgLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}