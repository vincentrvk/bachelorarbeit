/******************************************************************************
 *  CPI-Groovy Skript – SureTax Geocode Batch
 *
 *  Dieses Skript erfüllt folgende Aufgaben:
 *   1. Ermittlung aller benötigten Properties & Header
 *   2. Validierung des eingehenden Payloads
 *   3. Request-Mapping auf das SureTax-BatchGetGeocode-Format
 *   4. Aufruf der SureTax-API (HTTP-POST mit Basic-Auth)
 *   5. Response-Mapping auf das SAP-Zielschema
 *   6. Ausführliches Logging (Message-Attachments)
 *   7. Zentrales Error-Handling
 *
 *  Autor:  ChatGPT (Senior-Developer-Simulation)
 ******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.text.SimpleDateFormat
import java.nio.charset.StandardCharsets
import java.util.Base64

// ============================================================================
/*  Einstiegspunkt für das Skript                                                */
Message processData(Message message) {

    def messageLog = messageLogFactory?.getMessageLog(message)

    try {
        /* ------------------------------------------------------------------ */
        /* 1. Properties & Header einlesen                                    */
        def params = readContextParameters(message)

        /* ------------------------------------------------------------------ */
        /* 2. Eingehenden Payload laden und loggen                            */
        String inboundXml = message.getBody(String)
        logPayload(messageLog, '01_IncomingPayload', inboundXml)

        /* 3. Validierung                                                     */
        validateInput(inboundXml)

        /* ------------------------------------------------------------------ */
        /* 4. Request-Mapping erstellen                                       */
        String requestXml = mapRequest(inboundXml, params.username)
        logPayload(messageLog, '02_RequestPayload', requestXml)

        /* ------------------------------------------------------------------ */
        /* 5. API-Call                                                        */
        String responseXml = callSureTaxApi(requestXml, params, messageLog)
        logPayload(messageLog, '03_RawResponse', responseXml)

        /* ------------------------------------------------------------------ */
        /* 6. Response-Mapping                                                */
        String mappedResponse = mapResponse(responseXml, params.exchangejcdunifyind)
        logPayload(messageLog, '04_MappedResponse', mappedResponse)

        /* ------------------------------------------------------------------ */
        /* 7. Ergebnis in Message schreiben                                   */
        message.setBody(mappedResponse)
        return message

    } catch (Exception e) {
        /* jedes Mapping/Verarbeitungs-Problem wird hier abgefangen            */
        handleError(message.getBody(String), e, messageLog)
        // handleError wirft RuntimeException → return wird nicht erreicht
    }
}

/* ========================================================================== */
/*  Hilfs-Funktionen                                                           */
/* ========================================================================== */

/* -------------------------------------------------------------------------- */
/*  Liest Properties/Headers aus der Exchange-Message                          */
private Map readContextParameters(Message message) {

    String defaultValue = 'placeholder'

    String username              = getValue(message, 'sureTaxUsername', defaultValue)
    String password              = getValue(message, 'sureTaxPassword', defaultValue)
    String url                   = getValue(message, 'sureTaxURL',      defaultValue)
    String exchangejcdunifyind   = getValue(message, 'exchangejcdunifyind', defaultValue)

    return [
            username             : username,
            password             : password,
            url                  : url,
            exchangejcdunifyind  : exchangejcdunifyind
    ]
}

/* Holt Wert zuerst aus Property, danach aus Header, sonst Default */
private String getValue(Message msg, String key, String dflt) {
    def val = msg.getProperty(key) ?: msg.getHeader(key, String)
    (val == null || val.toString().trim().isEmpty()) ? dflt : val.toString()
}

/* -------------------------------------------------------------------------- */
/*  Validiert minimal das Eingangsdokument – COUNTRY ist required             */
private void validateInput(String xml) {
    def slurper = new XmlSlurper().parseText(xml)
    slurper.LOCATION_SIMPLE.each { loc ->
        if (!loc.COUNTRY?.text()) {
            throw new IllegalArgumentException("Validierung fehlgeschlagen: COUNTRY fehlt für SEQUENCE_NUM ${loc.SEQUENCE_NUM?.text()}")
        }
    }
}

/* -------------------------------------------------------------------------- */
/*  Request-Mapping                                                           */
private String mapRequest(String inXml, String clientNumber) {

    def inDoc = new XmlSlurper().parseText(inXml)
    def writer = new StringWriter()
    def ns3 = 'http://soa.noventic.com/GeocodeService/GeocodeService-V1'
    def year = new SimpleDateFormat('yyyy').format(new Date())

    new MarkupBuilder(writer).'ns3:BatchGetGeocode'('xmlns:ns3': ns3) {
        'ns3:request' {
            'ns3:ClientNumber'(clientNumber)
            'ns3:DataYear'(year)
            'ns3:LocationList' {
                inDoc.LOCATION_SIMPLE.each { loc ->
                    'ns3:Location' {
                        'ns3:SequenceNum'(loc.SEQUENCE_NUM.text().trim())
                        'ns3:Zipcode'(calculateZip(loc.ZIPCODE.text(), loc.COUNTRY.text()))
                        'ns3:AddressLine1'(loc?.STREET_ADDRESS_SIMPLE?.STREET?.text()?.trim() ?: '')
                    }
                }
            }
        }
    }
    return writer.toString()
}

/* Regelgemäße Bildung des Zip-Werts                                          */
private String calculateZip(String zipRaw, String countryRaw) {
    if (!zipRaw) { return '' }
    def country = (countryRaw ?: '').trim().toUpperCase()
    if (['US', 'USA'].contains(country)) {
        return zipRaw.trim()
    }
    // Anderes Land → Tokenize an "-"; erster Teil
    return zipRaw.tokenize('-')[0].trim()
}

/* -------------------------------------------------------------------------- */
/*  HTTP-Aufruf der SureTax-API                                               */
private String callSureTaxApi(String requestBody, Map params, def messageLog) {

    /* Basic-Auth Header erzeugen */
    String authToken = "${params.username}:${params.password}"
    String basicAuth = Base64.encoder.encodeToString(authToken.bytes)

    /* HTTP-Verbindung aufbauen */
    def urlConn = new URL(params.url).openConnection()
    urlConn.setRequestMethod('POST')
    urlConn.setDoOutput(true)
    urlConn.setRequestProperty('Authorization', "Basic ${basicAuth}")
    urlConn.setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')

    /* Request senden */
    urlConn.outputStream.withWriter('UTF-8') { it << requestBody }

    /* Response lesen */
    int rc = urlConn.responseCode
    String responsePayload = urlConn.inputStream.getText('UTF-8')

    /* Logging */
    messageLog?.addAttachmentAsString("HTTP-Status", rc.toString(), "text/plain")

    if (rc != 200) {
        throw new RuntimeException("SureTax API-Call fehlgeschlagen. HTTP-Status: ${rc}")
    }
    return responsePayload
}

/* -------------------------------------------------------------------------- */
/*  Response-Mapping                                                          */
private String mapResponse(String responseXml, String unifyInd) {

    def resp = new XmlSlurper().parseText(responseXml)
    def writer = new StringWriter()
    def ns2 = 'http://sap.com/xi/FotETaxUS'

    // State-Mapping Tabelle (Kurzbeispiel)
    def stateMap = [
            '01':'AL', '02':'AK', '03':'AZ', '04':'AR', '05':'CA', '06':'CO',
            '07':'CT', '08':'DE', '09':'FL', '10':'GA'   // … usw.
    ]

    new MarkupBuilder(writer).'ns2:TAX_JUR_REDEFINE_RECEIVE'('xmlns:ns2': ns2) {
        resp.'**'.findAll { it.name() == 'GeocodeResponse' }.each { geocodeResp ->
            String geocode   = geocodeResp.Geocode.text()
            String seqNum    = geocodeResp.SequenceNum.text()
            String respCode  = geocodeResp.ResponseCode.text()
            String errMsg    = geocodeResp.ErrorMessage.text()

            /* TXJCD-Ermittlung gem. Unify-Regeln                                */
            String txjcdValue = transformTxjcd(geocode, unifyInd, stateMap)

            'ns2:TAX_JURI_CODE_NUM' {
                'ns2:TXJCD'(txjcdValue)
                'ns2:SEQUENCE_NUM'(seqNum)
                'ns2:MSG_RETURN' {
                    'ns2:RETCODE'(respCode == '9999' ? '0'    : '1')
                    'ns2:ERRCODE'(respCode == '9999' ? '0000' : '1999')
                    'ns2:ERRMSG'(errMsg ?: '')
                }
            }
        }
    }
    return writer.toString()
}

/* Transformation der TXJCD gem. Vorgabe                                       */
private String transformTxjcd(String geocode, String unifyInd, Map stateMap) {

    if (unifyInd == 'X') {
        if (geocode.startsWith('ZZ')) {
            return 'US' + geocode
        }
        if (geocode.startsWith('US') && geocode.size() >= 4) {
            String stateDigits = geocode.substring(2,4)
            String mappedState = stateMap.get(stateDigits, stateDigits)
            return geocode[0..1] + mappedState + geocode.substring(4) + '-'
        }
    }
    return geocode
}

/* -------------------------------------------------------------------------- */
/*  Logging-Funktion – hängt Payload als String an die MPL                    */
private void logPayload(def messageLog, String name, String payload) {
    messageLog?.addAttachmentAsString(name, payload, 'text/xml')
}

/* -------------------------------------------------------------------------- */
/*  Globales Error-Handling                                                   */
private void handleError(String body, Exception e, def messageLog) {
    /* Payload anhängen, damit er im Monitoring verfügbar ist                 */
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: '', "text/xml")
    String errorMsg = "Fehler im Geocode-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}