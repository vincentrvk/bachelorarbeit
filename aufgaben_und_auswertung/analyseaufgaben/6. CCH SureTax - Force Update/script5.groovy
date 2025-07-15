/********************************************************************************************
*  S/4HANA → CCH Sure Tax – Force-Update IFlow
*
*  Modular Groovy-Script für SAP Cloud Integration (CPI)
*  -----------------------------------------------------
*  Aufgaben:
*    1.  Validierung erforderlicher Properties
*    2.  Request-Mapping der eingehenden XML-Struktur
*    3.  Aufruf der SureTax-API (POST)
*    4.  Response-Mapping
*    5.  Umfangreiches Logging & Error-Handling
*
*  Autor:  (Senior Integration Developer)
********************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.StreamingMarkupBuilder
import java.math.BigDecimal
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern

/********************************************************************************************
*  Einstiegsmethode
********************************************************************************************/
Message processData(Message message) {

    def messageLog = messageLogFactory?.getMessageLog(message)
    try {
        /*----------------------------------------------------*
         * 0. Vorbereitungen                                  *
         *----------------------------------------------------*/
        def incomingBody = message.getBody(String) ?: ''
        logAttachment(messageLog, '01-IncomingPayload', incomingBody)

        /*----------------------------------------------------*
         * 1. Properties / Header lesen & validieren          *
         *----------------------------------------------------*/
        def ctx = readContext(message)
        validateContext(ctx)

        /*----------------------------------------------------*
         * 2. Request-Mapping                                 *
         *----------------------------------------------------*/
        def requestXml = mapRequest(incomingBody, ctx)
        logAttachment(messageLog, '02-AfterRequestMapping', requestXml)

        /*----------------------------------------------------*
         * 3. API-Call „Force Update“                         *
         *----------------------------------------------------*/
        def responseBody = callSureTaxUpdate(ctx, requestXml)
        logAttachment(messageLog, '03-ResponseRaw', responseBody)

        /*----------------------------------------------------*
         * 4. Response-Mapping                                *
         *----------------------------------------------------*/
        def mappedResponse = mapResponse(responseBody, ctx)
        logAttachment(messageLog, '04-AfterResponseMapping', mappedResponse)

        /*----------------------------------------------------*
         * 5. Body ersetzen & fertig                          *
         *----------------------------------------------------*/
        message.setBody(mappedResponse)
        return message

    } catch (Exception e) {
        /** Fehlerbehandlung gem. Vorgabe **/
        handleError(message.getBody(String) as String, e, messageLog)
        return message   // wird nie erreicht, handleError wirft Exception
    }
}

/********************************************************************************************
*  Hilfsfunktionen
********************************************************************************************/

/**
 * Liest alle relevanten Properties & Header in eine Map und
 * befüllt nicht vorhandene Werte mit „placeholder“.
 */
private Map readContext(Message msg) {

    /* Helper-Closure */
    def fetch = { String key, boolean isHeader ->
        def val = isHeader ? msg.getHeader(key, String) : msg.getProperty(key)
        return (val != null && !val.toString().trim().isEmpty()) ? val.toString().trim() : 'placeholder'
    }

    return [
        sureTaxURL             : fetch('sureTaxURL',        false),
        sureTaxUsername        : fetch('sureTaxUsername',   false),
        sureTaxPassword        : fetch('sureTaxPassword',   false),
        exchangeCurrencyDecimal: fetch('exchageCurrencyDecimal', false),    // Schreibfehler in Aufgabenstellung beibehalten
        exchangeJcdUnifyInd    : fetch('exchangeJcdUnifyInd', false),
        exchangeTID            : fetch('exchangeTID',       false)
    ]
}

/**
 * Prüft das Vorhandensein der zwingenden Properties.
 * Wirft RuntimeException bei Fehler.
 */
private void validateContext(Map ctx) {
    if (ctx.exchangeJcdUnifyInd == 'placeholder') {
        throw new RuntimeException('Pflicht-Property [exchangeJcdUnifyInd] fehlt!')
    }
    if (ctx.exchangeTID == 'placeholder') {
        throw new RuntimeException('Pflicht-Property [exchangeTID] fehlt!')
    }
}

/**
 * Führt laut Vorgaben das Request-Mapping durch.
 * Liefert das fertige XML als String zurück.
 */
private String mapRequest(String sourceXml, Map ctx) {

    def slurper   = new XmlSlurper(false, false)
    def src       = slurper.parseText(sourceXml)
    def namespace = 'http://example.namespace/ns1'

    /* 1. DataYear ermitteln */
    def firstTaxDate = src.FORCE_ITEM[0]?.TAX_DATE?.text()
    def today        = LocalDate.now()
    def yearToUse    = firstTaxDate ? LocalDate.parse(firstTaxDate).year : today.year
    if (firstTaxDate && LocalDate.parse(firstTaxDate).isAfter(today)) {
        yearToUse = today.year
    }

    /* 2. Currency-Decimal allgemein (Header kann fehlen) */
    def defaultCurrDec = src.FORCE_HEADER?.CURR_DEC?.text() ?: '000'
    int headerCurrDec  = defaultCurrDec.isInteger() ? Integer.parseInt(defaultCurrDec) : 0

    /* 3. TaxAdjustmentItemList aufbauen */
    def items = src.FORCE_ITEM.collect { itm ->
        def currDecStr = itm.CURR_DEC?.text() ?: defaultCurrDec
        int currDec    = currDecStr.isInteger() ? Integer.parseInt(currDecStr) : 0
        BigDecimal factor = BigDecimal.valueOf(Math.pow(10, currDec))

        /* Hilfsfunktion zur Bereinigung & Vorzeichen */
        def cleanse = { String v ->
            if (!v) return ''
            return v.trim()
        }

        /* Werte lesen */
        BigDecimal taxamov    = new BigDecimal( cleanse(itm.TAXAMOV.text()    ?: '0')      )
        BigDecimal amount     = new BigDecimal( cleanse(itm.AMOUNT.text()     ?: '0')      )
        BigDecimal freight    = new BigDecimal( cleanse(itm.FREIGHT_AM.text() ?: '0')      )
        BigDecimal exempt     = new BigDecimal( cleanse(itm.EXEMPT_AMT.text() ?: '0')      )
        String creditInd      = cleanse(itm.CREDIT_IND.text() ?: '0')

        /* Revenue */
        BigDecimal revenue = (amount + freight - exempt) / factor
        if (!'0'.equalsIgnoreCase(creditInd)) {
            revenue = revenue * (-1)
        }

        /* Tax */
        BigDecimal tax = taxamov / factor
        if (!'0'.equalsIgnoreCase(creditInd)) {
            tax = tax * (-1)
        }

        /* Geocode */
        String geocode = cleanse(itm.TXJCD_POO.text())
        if ('X'.equalsIgnoreCase(ctx.exchangeJcdUnifyInd) && geocode.length() >= 4) {
            geocode = geocode[0..1] + geocode[4..-1]  // Zeichen 3 & 4 entfernen (Index 2 & 3)
        }

        /* Validierung Geocode */
        if (!Pattern.matches('^[A-Za-z]{2}[0-9]{5}[A-Za-z0-9]{5}$', geocode)) {
            throw new RuntimeException("Ungültiger Geocode [${geocode}]. Muss 12-stellig sein: AA99999XXXXX")
        }

        return [
            Revenue : revenue.setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString(),
            Tax     : tax.setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString(),
            Geocode : geocode
        ]
    }

    /* 4. XML generieren */
    def writer = new StringWriter()
    def builder = new StreamingMarkupBuilder()
    builder.encoding = 'UTF-8'
    writer << builder.bind {
        mkp.declareNamespace(ns1: namespace)
        'ns1:SoapTaxAdjustmentWithReturnFileCodeRequest' {
            'ns1:request' {
                'ns1:DataYear'( yearToUse )
                'ns1:TaxAdjustmentItemList' {
                    items.each { itm ->
                        'ns1:TaxAdjustmentItem' {
                            'ns1:Revenue'( itm.Revenue )
                            'ns1:Tax'(     itm.Tax     )
                            'ns1:OrderPlacementAddress' {
                                'ns1:Geocode'( itm.Geocode )
                            }
                        }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/**
 * Aufruf der SureTax-API via HTTP-POST inkl. Basic-Auth.
 * Liefert den Response-Body als String.
 */
private String callSureTaxUpdate(Map ctx, String payload) {

    URL url = new URL(ctx.sureTaxURL)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)

    /* Basic-Auth Header */
    String auth = ctx.sureTaxUsername + ':' + ctx.sureTaxPassword
    String encoded = auth.bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', 'Basic ' + encoded)
    conn.setRequestProperty('Content-Type', 'text/xml;charset=UTF-8')

    /* Payload senden */
    conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { it << payload }

    int rc = conn.responseCode
    String responseText = rc == 200 ?
            conn.inputStream.getText(StandardCharsets.UTF_8.name()) :
            conn.errorStream?.getText(StandardCharsets.UTF_8.name()) ?: ''

    if (rc != 200) {
        throw new RuntimeException("SureTax-API Antwort-Code ${rc}: ${responseText}")
    }
    return responseText
}

/**
 * Erzeugt das Ziel-XML gemäß RESPONSE-MAPPING Vorgabe.
 * Original-Response von SureTax wird aktuell nicht weiter-verarbeitet.
 */
private String mapResponse(String responseBody, Map ctx) {

    def namespace = 'http://sap.com/xi/FotETaxUS'
    def writer    = new StringWriter()
    def builder   = new StreamingMarkupBuilder()
    builder.encoding = 'UTF-8'

    writer << builder.bind {
        mkp.declareNamespace(ns2: namespace)
        'ns2:TAX_FORCE_RECEIVE' {
            'ns2:FORCE_RESULT_HEADER' {
                'ns2:API_VERSION'('1.0')
                'ns2:TID'( ctx.exchangeTID )
                'ns2:RETCODE'('0')
                'ns2:ERRCODE'('0000')
            }
        }
    }
    return writer.toString()
}

/**
 * Fügt dem Trace-Log ein Attachment hinzu – sofern im Log-Level aktiv.
 */
private void logAttachment(def messageLog, String name, String content) {
    try {
        messageLog?.addAttachmentAsString(name, content, 'text/xml')
    } catch (Exception ignore) {
        // Silent – Logging darf Fachlogik nicht stören
    }
}

/**
 * Einheitliches Error-Handling gem. Vorgaben.
 */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Force-Update-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}