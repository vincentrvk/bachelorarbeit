/******************************************************************************
 *  SAP Cloud Integration – Groovy-Skript
 *
 *  Zweck  :  Holen eines Geocode-Batches bei CCH Sure Tax
 *  Autor  :  (Senior Integration Developer)
 *  Stand  :  2025-06-16
 *
 *  Hinweise:
 *  – Das Skript ist vollständig modularisiert.
 *  – Jede Methode besitzt eine deutschsprachige Dokumentation.
 *  – Fehler werden zentral behandelt, der ursprüngliche Payload wird als
 *    Message-Attachment mitgegeben.
 ******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.StreamingMarkupBuilder
import groovy.xml.XmlUtil
import java.net.HttpURLConnection
import java.text.SimpleDateFormat
import java.util.Base64

Message processData(Message message) {

    /* ------------------------------------------------------------ *
     *  Technische Objekte initialisieren
     * ------------------------------------------------------------ */
    def messageLog = messageLogFactory.getMessageLog(message)

    /* ------------------------------------------------------------ *
     *  Hauptverarbeitung
     * ------------------------------------------------------------ */
    try {
        // 1. Header & Property-Handling
        def ctx            = prepareContext(message, messageLog)

        // 2. Request-Mapping
        def requestXml     = mapRequest(message.getBody(String), ctx, messageLog)

        // 3. API-Aufruf
        def responseXmlStr = callSureTax(requestXml, ctx, messageLog)

        // 4. Response-Mapping
        def finalPayload   = mapResponse(responseXmlStr, ctx, messageLog)

        // 5. Body setzen und zurück geben
        message.setBody(finalPayload)
        return message

    } catch (Exception e) {
        handleError(message.getBody(String), e, messageLog)
    }

}


/* ========================================================================== */
/* ===========================  Hilfsmethoden =============================== */
/* ========================================================================== */

/*-------------------------------------------------------------------------*
 | prepareContext                                                          |
 *-------------------------------------------------------------------------*/
/**
 * Liest benötigte Properties & Header aus dem SAP-CPI-Message-Objekt.
 * Fehlen Einträge, werden sie mit „placeholder“ vorbelegt.
 *
 * @return Map mit allen benötigten Kontextdaten.
 */
def prepareContext(Message message, def messageLog) {

    def body          = message.getBody(String)
    addAttachment(messageLog, 'IncomingPayload', body)

    // Properties lesen (oder default)
    def ctx = [:]
    ctx.sureTaxUsername     = message.getProperty('sureTaxUsername')     ?: 'placeholder'
    ctx.sureTaxPassword     = message.getProperty('sureTaxPassword')     ?: 'placeholder'
    ctx.sureTaxUrl          = message.getProperty('sureTaxURL')          ?: 'placeholder'
    ctx.exchangeJcdUnifyInd = message.getProperty('exchangejcdunifyind') ?: 'placeholder'

    return ctx
}

/*-------------------------------------------------------------------------*
 | mapRequest                                                              |
 *-------------------------------------------------------------------------*/
/**
 * Erstellt das Request-XML für den Sure Tax-Aufruf.
 *
 * @param rawIn  Eingehender S/4-Payload als String
 * @param ctx    Kontextdaten (Properties & Header)
 * @return       Serialisiertes XML als String
 */
def mapRequest(String rawIn, Map ctx, def messageLog) {

    // XML einlesen (Namespaces ignorieren)
    def inXml  = new XmlSlurper(false, false).parseText(rawIn)

    // Validierungen und Business Mapping
    def locationMaps = inXml.LOCATION_SIMPLE.collect { loc ->
        def country = loc.COUNTRY.text()?.trim()
        if (!country) {
            throw new IllegalArgumentException('Validierung fehlgeschlagen: COUNTRY ist leer.')
        }

        def seqNum  = loc.SEQUENCE_NUM.text()?.trim()
        def zipIn   = loc.ZIPCODE.text()?.trim()
        def street  = loc.STREET_ADDRESS_SIMPLE?.STREET?.text()?.trim()

        // ZIP-Logik
        def zipOut
        if (country?.equalsIgnoreCase('US') || country?.equalsIgnoreCase('USA')) {
            zipOut = zipIn
        } else {
            zipOut = (zipIn ?: '').tokenize('-')[0]?.trim()
        }

        [
            sequenceNum   : seqNum,
            zipcode       : zipOut,
            addressLine1  : street
        ]
    }

    // Request-XML mit StreamingMarkupBuilder erzeugen
    def writer = new StringWriter()
    def builder = new StreamingMarkupBuilder(encoding: 'UTF-8')

    writer << builder.bind {
        mkp.declareNamespace(ns3: 'http://soa.noventic.com/GeocodeService/GeocodeService-V1')
        'ns3:BatchGetGeocode' {
            'ns3:request' {
                'ns3:ClientNumber'(ctx.sureTaxUsername)
                'ns3:DataYear'(new Date().format('yyyy'))
                'ns3:LocationList' {
                    locationMaps.each { l ->
                        'ns3:Location' {
                            'ns3:SequenceNum'(l.sequenceNum)
                            'ns3:Zipcode'(l.zipcode)
                            'ns3:AddressLine1'(l.addressLine1)
                        }
                    }
                }
            }
        }
    }

    def requestXml = XmlUtil.serialize(writer.toString())
    addAttachment(messageLog, 'AfterRequestMapping', requestXml)
    return requestXml
}

/*-------------------------------------------------------------------------*
 | callSureTax                                                             |
 *-------------------------------------------------------------------------*/
/**
 * Führt den HTTP-POST gegen die Sure Tax-API aus.
 *
 * @param requestBody XML-String aus dem Request-Mapping
 * @param ctx         Kontextdaten (Properties)
 * @return            Response-Payload als String
 */
def callSureTax(String requestBody, Map ctx, def messageLog) {

    def url = new URL(ctx.sureTaxUrl)
    HttpURLConnection con = (HttpURLConnection) url.openConnection()
    con.with {
        setRequestMethod('POST')
        setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
        def token = "${ctx.sureTaxUsername}:${ctx.sureTaxPassword}".bytes.encodeBase64().toString()
        setRequestProperty('Authorization', "Basic ${token}")
        setDoOutput(true)
        outputStream.withWriter('UTF-8') { it << requestBody }
    }

    int rc = con.responseCode
    def responseStr = rc < 300   ? con.inputStream.getText('UTF-8')
                                 : con.errorStream?.getText('UTF-8')
    if (rc >= 300) {
        throw new RuntimeException("HTTP-Fehler bei Sure Tax-Aufruf (Status ${rc}) – Payload: ${responseStr}")
    }

    addAttachment(messageLog, 'ResponsePayload', responseStr)
    return responseStr
}

/*-------------------------------------------------------------------------*
 | mapResponse                                                             |
 *-------------------------------------------------------------------------*/
/**
 * Wandelt den Sure Tax-Response in das S/4-Zielformat.
 *
 * @param responseXmlStr Response-XML als String
 * @param ctx            Kontextdaten
 * @return               Gemapptes XML-String für S/4
 */
def mapResponse(String responseXmlStr, Map ctx, def messageLog) {

    def resp = new XmlSlurper(false, false).parseText(responseXmlStr)

    // State-Mapping (lokal, da keine globalen Konstanten erlaubt)
    def stateMap = [
        '01':'AL','02':'AK','04':'AZ','05':'AR','06':'CA','08':'CO','09':'CT','10':'DE',
        '11':'DC','12':'FL','13':'GA','15':'HI','16':'ID','17':'IL','18':'IN','19':'IA',
        '20':'KS','21':'KY','22':'LA','23':'ME','24':'MD','25':'MA','26':'MI','27':'MN',
        '28':'MS','29':'MO','30':'MT','31':'NE','32':'NV','33':'NH','34':'NJ','35':'NM',
        '36':'NY','37':'NC','38':'ND','39':'OH','40':'OK','41':'OR','42':'PA','44':'RI',
        '45':'SC','46':'SD','47':'TN','48':'TX','49':'UT','50':'VT','51':'VA','53':'WA',
        '54':'WV','55':'WI','56':'WY'
    ]

    // Gemapte Einträge sammeln
    def entries = resp.BatchGetGeocodeResult.GeocodeResponseList.GeocodeResponse.collect { g ->
        def geo  = g.Geocode.text()
        def seq  = g.SequenceNum.text()
        def code = g.ResponseCode.text()
        def msg  = g.ErrorMessage.text()

        // TXJCD-Ermittlung
        def txjcd
        if (ctx.exchangeJcdUnifyInd == 'X') {
            if (geo.startsWith('ZZ')) {
                txjcd = 'US' + geo
            } else if (geo.startsWith('US') && geo.size() >= 4) {
                def stateKey  = geo.substring(2,4)
                def stateAbbr = stateMap[stateKey] ?: stateKey
                txjcd = 'US' + stateAbbr + geo.substring(2) + '-'
            } else {
                txjcd = geo
            }
        } else {
            txjcd = geo
        }

        // Ret-/Err-Code
        def retCode = code == '9999' ? '0'    : '1'
        def errCode = code == '9999' ? '0000' : '1999'

        [
            txjcd      : txjcd,
            sequence   : seq,
            retCode    : retCode,
            errCode    : errCode,
            errMsg     : msg
        ]
    }

    // Ziel-XML erstellen
    def writer = new StringWriter()
    def builder = new StreamingMarkupBuilder(encoding: 'UTF-8')
    writer << builder.bind {
        mkp.declareNamespace(ns2: 'http://sap.com/xi/FotETaxUS')
        'ns2:TAX_JUR_REDEFINE_RECEIVE' {
            entries.each { e ->
                'ns2:TAX_JURI_CODE_NUM' {
                    'ns2:TXJCD'(e.txjcd)
                    'ns2:SEQUENCE_NUM'(e.sequence)
                    'ns2:MSG_RETURN' {
                        'ns2:RETCODE'(e.retCode)
                        'ns2:ERRCODE'(e.errCode)
                        'ns2:ERRMSG'(e.errMsg)
                    }
                }
            }
        }
    }

    def outXml = XmlUtil.serialize(writer.toString())
    addAttachment(messageLog, 'AfterResponseMapping', outXml)
    return outXml
}

/*-------------------------------------------------------------------------*
 | handleError                                                             |
 *-------------------------------------------------------------------------*/
/**
 * Zentrales Error-Handling. Fügt den fehlerhaften Payload als Attachment an
 * und wirft eine RuntimeException weiter, damit die CPI das IFlow beendet.
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

/*-------------------------------------------------------------------------*
 | addAttachment                                                           |
 *-------------------------------------------------------------------------*/
/**
 * Fügt dem MessageLog ein Attachment hinzu (sofern MessageLog vorhanden).
 */
def addAttachment(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content ?: '', 'text/xml')
}