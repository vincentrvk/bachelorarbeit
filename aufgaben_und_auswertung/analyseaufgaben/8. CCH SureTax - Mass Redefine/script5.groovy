/*
 *  CPI – Groovy Script  :  S/4H → CCH SureTax – Batch-Geocode
 *
 *  Anforderungen (gekürzt):
 *    • Modularer Aufbau
 *    • Kommentierung (DE)
 *    • Validierung, Mapping, HTTP-Call, Error-Handling, Logging
 */

import com.sap.gateway.ip.core.customdev.util.Message
import java.text.SimpleDateFormat
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.net.URL

/********************************************************************************************************************
 *  Haupt-Einstiegspunkt
 *******************************************************************************************************************/
Message processData(Message message) {

    // Message Logger
    def msgLog = messageLogFactory.getMessageLog(message)

    try {
        /*----------------------------------------------------------------
         *  1) Header / Property Handling
         *---------------------------------------------------------------*/
        def env = initEnvironment(message)

        /*----------------------------------------------------------------
         *  2) Eingehenden Payload loggen
         *---------------------------------------------------------------*/
        logStep(msgLog, '01-IncomingPayload', env.body)

        /*----------------------------------------------------------------
         *  3) Request-Mapping
         *---------------------------------------------------------------*/
        def requestXml = buildRequestPayload(env)
        logStep(msgLog, '02-Request-Mapping', requestXml)

        /*----------------------------------------------------------------
         *  4) API-Call
         *---------------------------------------------------------------*/
        def responseXml = callSureTax(requestXml, env)
        logStep(msgLog, '03-ResponsePayload', responseXml)

        /*----------------------------------------------------------------
         *  5) Response-Mapping
         *---------------------------------------------------------------*/
        def targetXml = buildResponsePayload(responseXml, env)
        logStep(msgLog, '04-Response-Mapping', targetXml)

        /*----------------------------------------------------------------
         *  6) Body for CPI weitergeben
         *---------------------------------------------------------------*/
        message.setBody(targetXml)
        return message

    } catch (Exception e) {
        // Einmal das ursprünglich eingehende Payload anhängen
        handleError(message.getBody(String) as String, e, msgLog)
        // handleError wirft gezielt RuntimeException
    }
}


/********************************************************************************************************************
 *  Funktion:  initEnvironment  –  Header / Property Handling & Vorbelegung
 *******************************************************************************************************************/
def initEnvironment(Message message) {

    /*---------------------------------------------------------
     *  Properties / Header aus Message oder Placeholder
     *--------------------------------------------------------*/
    def getProp  = { String key, String defVal -> message.getProperty(key)  ?: defVal }
    def getHead  = { String key, String defVal -> message.getHeader(key, String.class) ?: defVal }

    /* Pflicht-Properties */
    def env = [:]
    env.body             = message.getBody(String) ?: ''
    env.username         = getProp('sureTaxUsername',    'placeholder')
    env.password         = getProp('sureTaxPassword',    'placeholder')
    env.url              = getProp('sureTaxURL',         'https://placeholder')
    env.exchangejcdFlag  = getProp('exchangejcdunifyind','')
    env.stateMap         = getProp('stateMap',           [
                                "00":"US","01":"AL","02":"AK","03":"AL",
                                "04":"AZ","05":"AR","06":"CA"
                            ])
    return env
}


/********************************************************************************************************************
 *  Funktion:  buildRequestPayload
 *******************************************************************************************************************/
String buildRequestPayload(Map env) {

    // Datum – nur Jahr
    def dataYear = new SimpleDateFormat('yyyy').format(new Date())

    /*---------------------------------------------------------
     *  Eingehendes XML parsen
     *--------------------------------------------------------*/
    def src      = new XmlSlurper().parseText(env.body)
    def nsFot    = new groovy.xml.Namespace('http://sap.com/xi/FotETaxUS','n0')

    /*---------------------------------------------------------
     *  Validierung – COUNTRY ist required
     *--------------------------------------------------------*/
    src.LOCATION_SIMPLE.each { loc ->
        if (!(loc.COUNTRY?.text())) {
            throw new IllegalArgumentException('Validierung fehlgeschlagen: COUNTRY ist erforderlich.')
        }
    }

    /*---------------------------------------------------------
     *  Ziel XML aufbauen
     *--------------------------------------------------------*/
    def writer = new StringWriter()
    def ns3 = 'http://soa.noventic.com/GeocodeService/GeocodeService-V1'
    def builder = new groovy.xml.MarkupBuilder(writer)
    builder.'ns3:BatchGetGeocode'('xmlns:ns3':ns3) {
        'ns3:request' {
            'ns3:ClientNumber'(env.username)
            'ns3:DataYear'(dataYear)
            'ns3:LocationList' {
                src.LOCATION_SIMPLE.each { loc ->
                    'ns3:Location' {
                        // SEQUENCE_NUM
                        def seq = loc.SEQUENCE_NUM.text().trim()
                        'ns3:SequenceNum'(seq)

                        // ZIPCODE Transformation
                        def country = loc.COUNTRY.text()?.trim()
                        def zip     = loc.ZIPCODE.text()?.trim()
                        def zipTarget
                        if (country?.equalsIgnoreCase('US') || country?.equalsIgnoreCase('USA')) {
                            zipTarget = zip
                        } else {
                            zipTarget = zip?.tokenize('-')?.first()?.trim()
                        }
                        'ns3:Zipcode'(zipTarget ?: '')

                        // STREET
                        def street = loc.STREET_ADDRESS_SIMPLE?.STREET?.text()?.trim()
                        'ns3:AddressLine1'(street ?: '')
                    }
                }
            }
        }
    }
    return writer.toString()
}


/********************************************************************************************************************
 *  Funktion:  callSureTax  –  HTTP POST mit Basic-Auth
 *******************************************************************************************************************/
String callSureTax(String requestXml, Map env) {

    /* Verbindung herstellen */
    URL url = new URL(env.url)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.with {
        requestMethod       = 'POST'
        doOutput            = true
        connectTimeout      = 30000
        readTimeout         = 30000
        // Basic-Auth
        def authString      = "${env.username}:${env.password}"
        def encodedAuth     = authString.bytes.encodeBase64().toString()
        setRequestProperty('Authorization', "Basic ${encodedAuth}")
        setRequestProperty('Content-Type',  'text/xml; charset=UTF-8')
    }

    // Request Body schicken
    conn.outputStream.withWriter('UTF-8') { it << requestXml }

    // HTTP-Statuscode prüfen
    int rc = conn.responseCode
    if (rc != 200) {
        def err = conn.errorStream?.text
        throw new RuntimeException("SureTax-Call fehlgeschlagen – HTTP ${rc}. ${err ?: ''}")
    }

    // Antwort zurückgeben
    return conn.inputStream.getText('UTF-8')
}


/********************************************************************************************************************
 *  Funktion:  buildResponsePayload
 *******************************************************************************************************************/
String buildResponsePayload(String responseXml, Map env) {

    def src    = new XmlSlurper().parseText(responseXml)
    def ns0    = new groovy.xml.Namespace('http://soa.noventic.com/GeocodeService/GeocodeService-V1','ns0')

    // Ziel XML generieren
    def writer = new StringWriter()
    def ns2    = 'http://sap.com/xi/FotETaxUS'
    def builder = new groovy.xml.MarkupBuilder(writer)
    builder.'ns2:TAX_JUR_REDEFINE_RECEIVE'('xmlns:ns2':ns2) {

        src.'ns0:BatchGetGeocodeResult'.'ns0:GeocodeResponseList'.'ns0:GeocodeResponse'.each { resp ->

            def geocode     = resp.'ns0:Geocode'    .text()
            def seqNum      = resp.'ns0:SequenceNum'.text()
            def respCode    = resp.'ns0:ResponseCode'.text()
            def errMsg      = resp.'ns0:ErrorMessage'.text()

            // TXJCD Ermittlung
            def txjcd = geocode
            if (env.exchangejcdFlag == 'X') {
                if (geocode?.startsWith('ZZ')) {
                    txjcd = 'US' + geocode
                } else if (geocode?.startsWith('US')) {
                    // Zeichen 3-4 – StateCode
                    def stateNum = geocode.substring(2,4)
                    def mappedState = env.stateMap[stateNum] ?: stateNum
                    txjcd = geocode.substring(0,2) + mappedState + geocode.substring(2) + '-'
                }
            }

            /*-----------------------------------------------------
             *  Ziel Struktur
             *----------------------------------------------------*/
            'ns2:TAX_JURI_CODE_NUM' {
                'ns2:TXJCD'(txjcd)
                'ns2:SEQUENCE_NUM'(seqNum)
                'ns2:MSG_RETURN' {
                    // RETCODE
                    'ns2:RETCODE'( respCode == '9999' ? '0' : '1')
                    // ERRCODE
                    'ns2:ERRCODE'( respCode == '9999' ? '0000' : '1999')
                    // ERRMSG
                    'ns2:ERRMSG'( errMsg ?: '' )
                }
            }
        }
    }
    return writer.toString()
}


/********************************************************************************************************************
 *  Funktion:  logStep  –  Attachment Logging
 *******************************************************************************************************************/
void logStep(def msgLog, String name, String content) {
    msgLog?.addAttachmentAsString(name, content ?: '', 'text/xml')
}


/********************************************************************************************************************
 *  Funktion:  handleError  –  zentrales Error-Handling
 *******************************************************************************************************************/
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}