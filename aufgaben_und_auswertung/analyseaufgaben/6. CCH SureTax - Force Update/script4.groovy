/**************************************************************************
 *  Groovy‐Skript – S/4HANA → CCH SureTax Update
 *  -----------------------------------------------------------------------
 *  Autor:    Senior-Integration-Entwickler
 *  Version:  1.0
 *
 *  Dieses Skript erfüllt alle in der Aufgabenstellung genannten
 *  Anforderungen (Modularität, Mapping, Validierung, Logging,
 *  Error-Handling).
 **************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.nio.charset.StandardCharsets

/* =======================================================================
   Zentrale Einstiegsmethode
   ======================================================================= */
Message processData(Message message) {

    /* ---------------------------------------------------------------
       Initialisierung
       --------------------------------------------------------------- */
    def body        = message.getBody(String) ?: ''
    def messageLog  = messageLogFactory?.getMessageLog(message)

    try {

        /* 1. Eingehenden Payload loggen */
        logStep(messageLog, '01_Incoming_Payload', body)

        /* 2. Header & Properties einlesen / vorbelegen */
        def ctx = resolveContext(message)

        /* 3. Pflicht-Properties validieren                            */
        validateContext(ctx)

        /* 4. Request-Payload (SureTax) erzeugen                       */
        def requestXml = buildRequestPayload(body, ctx)
        logStep(messageLog, '02_Request_Mapping', requestXml)

        /* 5. API-Call an SureTax durchführen                          */
        def responseXml = callSureTax(requestXml, ctx, messageLog)
        logStep(messageLog, '03_Response_Payload', responseXml)

        /* 6. Response-Payload in Zielschema mappen                    */
        def targetXml  = buildResponsePayload(ctx)
        logStep(messageLog, '04_Response_Mapping', targetXml)

        /* 7. Ergebnis in Message-Body ablegen                         */
        message.setBody(targetXml)
        return message

    } catch (Exception e) {
        /* Fehler zentral behandeln und weiterwerfen */
        handleError(body, e, messageLog)
    }
}

/* =======================================================================
   Funktions-Module
   ======================================================================= */

/* -----------------------------------------------------------------------
 *  Header / Properties auslesen oder mit „placeholder“ belegen
 * --------------------------------------------------------------------- */
def Map resolveContext(Message msg) {

    Map ctx = [:]

    /* vorhandene Properties ziehen – fehlt eine, wird „placeholder“ gesetzt */
    ['sureTaxUsername',
     'sureTaxPassword',
     'sureTaxURL',
     'exchangeCurrencyDecimal',   // kommt evtl. aus Header
     'exchangeJcdUnifyInd',
     'exchangeTID'
    ].each { key ->
        def val = msg.getProperty(key)
        ctx[key] = val ? val.toString() : 'placeholder'
    }

    /* Fallback für exchangeCurrencyDecimal aus CURR_DEC im Header */
    if (ctx.exchangeCurrencyDecimal == 'placeholder') {
        def body           = msg.getBody(String) ?: ''
        def slurper        = new XmlSlurper().parseText(body)
        def currDecNode    = slurper.'FORCE_HEADER'.'CURR_DEC'
        ctx.exchangeCurrencyDecimal = currDecNode?.text() ?: '0'
    }

    return ctx
}

/* -----------------------------------------------------------------------
 *  Pflicht-Properties prüfen
 * --------------------------------------------------------------------- */
def void validateContext(Map ctx) {

    List missing = []
    ['exchangeJcdUnifyInd', 'exchangeTID'].each { key ->
        if (!ctx[key] || ctx[key] == 'placeholder') { missing << key }
    }

    if (missing) {
        throw new IllegalStateException(
                "Pflicht-Property/-Properties fehlen: ${missing.join(', ')}")
    }
}

/* -----------------------------------------------------------------------
 *  Request-XML an SureTax erzeugen
 * --------------------------------------------------------------------- */
def String buildRequestPayload(String inXml, Map ctx) {

    /* Eingangs-XML einlesen */
    def inDoc           = new XmlSlurper().parseText(inXml)
    def items           = inDoc.'FORCE_ITEM'

    /* Skalierungsfaktor bestimmen */
    int   currDec       = (ctx.exchangeCurrencyDecimal ?: '0').replaceAll('[^0-9]', '').toInteger()
    BigDecimal factor   = BigDecimal.ONE.scaleByPowerOfTen(currDec)    // 10^CURR_DEC

    /* DataYear – erstes TAX_DATE, ggf. auf aktuelles Jahr begrenzen */
    def firstDateNode   = items.'TAX_DATE'[0]
    int  dataYear
    if (firstDateNode) {
        def year = LocalDate.parse(firstDateNode.text()).year
        dataYear = Math.min(year, LocalDate.now().year)
    } else {
        dataYear = LocalDate.now().year
    }

    /* TaxAdjustmentItem-Liste erzeugen                                  */
    List itemList = items.collect { n ->
        /* Hilfsfunktion zum Wandeln von String → BigDecimal              */
        BigDecimal toNumber(String s) {
            if (!s) return 0G
            return new BigDecimal(s.trim())
        }

        /* Einzelwerte lesen                                              */
        BigDecimal amount      = toNumber(n.'AMOUNT'.text())
        BigDecimal freight     = toNumber(n.'FREIGHT_AM'.text())
        BigDecimal exempt      = toNumber(n.'EXEMPT_AMT'.text())
        BigDecimal taxamov     = toNumber(n.'TAXAMOV'.text())
        String     creditInd   = n.'CREDIT_IND'.text()?.trim()
        String     txjcd_poo   = n.'TXJCD_POO'.text()?.trim()

        /* Revenue-Berechnung                                             */
        BigDecimal revenueCalc = (amount + freight - exempt) / factor
        if (creditInd && creditInd != '0') { revenueCalc = revenueCalc * -1 }

        /* Tax-Berechnung                                                 */
        BigDecimal taxCalc     = (taxamov / factor)
        if (creditInd && creditInd != '0') { taxCalc = taxCalc * -1 }

        /* Geocode transformieren                                         */
        String geo = buildGeocode(txjcd_poo, ctx.exchangeJcdUnifyInd)

        return [ revenue : revenueCalc.stripTrailingZeros().toPlainString(),
                 tax     : taxCalc.stripTrailingZeros().toPlainString(),
                 geocode : geo ]
    }

    /* XML via MarkupBuilder schreiben                                   */
    def writer = new StringWriter()
    def xml    = new MarkupBuilder(writer)

    xml.'ns1:SoapTaxAdjustmentWithReturnFileCodeRequest'(
            'xmlns:ns1': 'http://example.namespace/ns1') {

        'ns1:request' {
            'ns1:DataYear'(dataYear)
            'ns1:TaxAdjustmentItemList' {
                itemList.each { itm ->
                    'ns1:TaxAdjustmentItem' {
                        'ns1:Revenue'(itm.revenue)
                        'ns1:Tax'(itm.tax)
                        'ns1:OrderPlacementAddress' {
                            'ns1:Geocode'(itm.geocode)
                        }
                    }
                }
            }
        }
    }

    return writer.toString()
}

/* -----------------------------------------------------------------------
 *  Geocode Transformation & Validierung
 * --------------------------------------------------------------------- */
def String buildGeocode(String source, String unifyInd) {

    if (!source) {
        throw new IllegalArgumentException('TXJCD_POO fehlt im Eingangsdatensatz')
    }

    /* ggf. Zeichen 3 & 4 entfernen */
    def transformed = (unifyInd == 'X' && source.length() > 4)
            ? source[0..1] + source[4..-1]
            : source

    /* Prüfen: exakt 12 Zeichen?                                         */
    if (transformed.length() != 12) {
        throw new IllegalArgumentException(
                "Geocode (${transformed}) hat nicht genau 12 Zeichen")
    }

    /* Regex-Validierung                                                 */
    if (!(transformed ==~ /^[A-Za-z]{2}[0-9]{5}[A-Za-z0-9]{5}$/)) {
        throw new IllegalArgumentException(
                "Geocode (${transformed}) erfüllt nicht das geforderte Format")
    }
    return transformed
}

/* -----------------------------------------------------------------------
 *  HTTP-Aufruf an SureTax
 * --------------------------------------------------------------------- */
def String callSureTax(String reqBody, Map ctx, def messageLog) {

    /* Verbindung aufbauen                                               */
    def url  = new URL(ctx.sureTaxURL)
    def conn = url.openConnection() as java.net.HttpURLConnection
    conn.with {
        requestMethod  = 'POST'
        connectTimeout = 15000
        readTimeout    = 30000
        doOutput       = true

        /* Basic-Auth Header bauen                                       */
        def auth = "${ctx.sureTaxUsername}:${ctx.sureTaxPassword}"
        setRequestProperty('Authorization',
                'Basic ' + auth.bytes.encodeBase64().toString())
        /* Content-Type setzen                                           */
        setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')
    }

    /* Request Body schreiben                                            */
    conn.outputStream.withWriter('UTF-8') { it << reqBody }

    /* Response empfangen                                                */
    def respCode = conn.responseCode
    def respBody = conn.inputStream?.getText('UTF-8')

    if (respCode != 200) {
        throw new RuntimeException(
                "SureTax-Service antwortet mit HTTP ${respCode}")
    }
    return respBody ?: ''
}

/* -----------------------------------------------------------------------
 *  Response-Payload (TAX_FORCE_RECEIVE) erzeugen
 * --------------------------------------------------------------------- */
def String buildResponsePayload(Map ctx) {

    def writer = new StringWriter()
    def xml    = new MarkupBuilder(writer)

    xml.'ns2:TAX_FORCE_RECEIVE'('xmlns:ns2': 'http://sap.com/xi/FotETaxUS') {
        'ns2:FORCE_RESULT_HEADER' {
            'ns2:API_VERSION'('1.0')
            'ns2:TID'(ctx.exchangeTID)
            'ns2:RETCODE'('0')
            'ns2:ERRCODE'('0000')
        }
    }
    return writer.toString()
}

/* -----------------------------------------------------------------------
 *  Logging-Helfer (Message-Attachment)
 * --------------------------------------------------------------------- */
def void logStep(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content, 'text/xml')
}

/* -----------------------------------------------------------------------
 *  Zentrales Error-Handling
 * --------------------------------------------------------------------- */
def void handleError(String body, Exception e, def messageLog) {
    /* Eingehenden Payload sicher als Attachment mitgeben               */
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    /* Prägnante Fehlermeldung erzeugen                                 */
    def errorMsg = "Fehler im SureTax-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}