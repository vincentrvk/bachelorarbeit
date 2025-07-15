/*****************************************************************************************
 *  IFlow-Groovy-Script:  S/4HANA Cloud  ->  CCH Sure Tax – Tax Calculation
 *
 *  Autor :  ChatGPT  (Senior-Integration-Developer)
 *  Datum :  2025-06-16
 *
 *  Hinweis:
 *  --------
 *  • Das Skript erfüllt die in der Aufgabenbeschreibung definierten Modularitäts-,
 *    Logging-, Mapping- und Error-Handling-Anforderungen.
 *  • Jeder Verarbeitungsschritt ist in einer eigene Methode gekapselt und kommentiert.
 *  • Alle Attachments werden als String angehängt, damit sie bequem im MPL
 *    eingesehen werden können.
 *****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import java.net.HttpURLConnection
import java.text.DecimalFormat
import groovy.xml.MarkupBuilder
import groovy.xml.XmlSlurper

// Haupteinstiegspunkt des Groovy-Skriptes -------------------------------------------------
Message processData(Message message) {

    // Initialisierung des MPL-Loggers (kann in Non-MPL-Unit-Tests null sein)
    def messageLog = messageLogFactory?.getMessageLog(message)

    // Ursprünglicher Request-Body
    String incomingPayload = message.getBody(String)

    // Alle weiteren Schritte in einem try/catch für zentrales Error-Handling
    try {

        // 1) Logging des eingehenden Payloads
        logStep(messageLog, '01_Incoming_Payload', incomingPayload)

        // 2) Header & Properties einlesen / vorbereiten
        Map ctx = readContext(message)

        // 3) Request-Mapping SureTax
        String requestXml = mapRequest(incomingPayload, ctx)
        logStep(messageLog, '02_Request_Mapped', requestXml)

        // 4) Aufruf SureTax-SOAP-Service
        String responseXml = callSureTax(requestXml, ctx, messageLog)
        logStep(messageLog, '03_SureTax_Response_Raw', responseXml)

        // 5) Response-Mapping zurück ins SAP-Zielformat
        String mappedResponse = mapResponse(responseXml, ctx)
        logStep(messageLog, '04_Response_Mapped', mappedResponse)

        // 6) Mapped Payload als neuen Body setzen
        message.setBody(mappedResponse)
        return message

    } catch (Exception e) {
        // Zentrales Error-Handling
        handleError(incomingPayload, e, messageLog)
    }
}



// =================================================================================================
// 1. Kontext-Ermittlung (Headers & Properties)
// =================================================================================================
/** Liest alle relevanten Header & Properties; fehlende Werte werden mit 'placeholder' ersetzt. */
Map readContext(Message msg) {

    Map ctx = [:]

    // Properties -------------------------------------------------------------------------
    ctx.sureTaxUsername          = getVal(msg.getProperty('sureTaxUsername'))
    ctx.sureTaxPassword          = getVal(msg.getProperty('sureTaxPassword'))
    ctx.sureTaxURL               = getVal(msg.getProperty('sureTaxURL'))
    ctx.exchageCurrencyDecimal   = getVal(msg.getProperty('exchageCurrencyDecimal'))      // Schreibfehler im Requirement beibehalten
    ctx.exchangeCurrencyDecimal  = getVal(msg.getProperty('exchangeCurrencyDecimal'))     // Für Response-Mapping
    ctx.exchangeTransTypeCode    = getVal(msg.getProperty('exchangeTransTypeCode'))
    ctx.exchangeTTCodeValueMap   = msg.getProperty('exchangeTTCodeValueMap') instanceof Map ?
                                   msg.getProperty('exchangeTTCodeValueMap') : [:]

    // Header (könnten in manchen Szenarien gesetzt sein) ---------------------------------
    // … addHeader falls benötigt – aktuell keine Header laut Anforderung

    return ctx
}

/** Liefert String-Wert oder 'placeholder', falls null/leer. */
private static String getVal(def value) {
    (value == null || value.toString().trim().isEmpty()) ? 'placeholder' : value.toString().trim()
}



// =================================================================================================
// 2. Request-Mapping  (S/4 -> SureTax)
// =================================================================================================
/** Führt das komplette Request-Mapping gemäss Vorgabe durch und liefert das SOAP-Request-XML. */
String mapRequest(String xmlString, Map ctx) {

    def src  = new XmlSlurper().parseText(xmlString)
    String currDecStr = src?.CALCULATION_HEADER?.CURR_DEC?.text()?.trim() ?: '02'
    int    currDecInt = safeParseInt(currDecStr, 2)
    BigDecimal scale  = 10G ** currDecInt                 // 10^n  (G für Groovy-BigDecimal-Literal)

    BigDecimal totalRevenue = 0G
    List<Map>  itemsMapped  = []

    src.CALCULATION_ITEM.each { item ->

        String itemNo     = item.ITEM_NO.text().trim()
        BigDecimal amount = parseScaled(item.AMOUNT.text(), currDecInt)
        BigDecimal freight= parseScaled(item.FREIGHT_AM.text(), currDecInt)
        BigDecimal revenue= amount + freight

        // Summierung für TotalRevenue
        totalRevenue += revenue

        // transTypeCode Mapping
        String transTypeCode = ''
        if (ctx.exchangeTTCodeValueMap.containsKey(itemNo)) {
            transTypeCode = ctx.exchangeTTCodeValueMap[itemNo]
        } else if (!ctx.exchangeTransTypeCode.equals('placeholder')) {
            transTypeCode = ctx.exchangeTransTypeCode
        }

        // Tax-Exemption-Code
        BigDecimal exemptAmt = parseScaled(item.EXEMPT_AMT.text(), currDecInt)
        String exemptionCode
        if (exemptAmt.compareTo(BigDecimal.ZERO) == 0) {
            exemptionCode = '00'
        } else {
            BigDecimal percent = (exemptAmt.abs() / (amount.abs() ?: 1G)).setScale(4, BigDecimal.ROUND_HALF_EVEN)
            exemptionCode = "06:${trimTrailingZeros(percent)}"
        }

        itemsMapped << [
                transTypeCode : transTypeCode,
                revenue       : revenue.setScale(currDecInt, BigDecimal.ROUND_HALF_EVEN).toString(),
                exemptionCode : exemptionCode
        ]
    }

    // SOAP-Envelope via MarkupBuilder ----------------------------------------------------
    StringWriter sw = new StringWriter()
    MarkupBuilder mb = new MarkupBuilder(sw)

    mb.'soapenv:SoapRequest'('xmlns:soapenv':'http://namespace-placeholder') {
        'soapenv:request' {
            'soapenv:CurrencyDecimal'(currDecStr)
            'soapenv:TotalRevenue'( totalRevenue.setScale(currDecInt, BigDecimal.ROUND_HALF_EVEN).toString() )
            'soapenv:ItemList' {
                itemsMapped.each { itm ->
                    'soapenv:Item' {
                        'soapenv:transTypeCode'(itm.transTypeCode)
                        'soapenv:Revenue'(itm.revenue)
                        'soapenv:TaxExemptionCodeList' {
                            'soapenv:string'(itm.exemptionCode)
                        }
                    }
                }
            }
        }
    }
    return sw.toString()
}



// =================================================================================================
// 3. Aufruf SureTax-API
// =================================================================================================
/** Führt den HTTP-POST gegen SureTax aus und liefert die Response als String zurück. */
String callSureTax(String requestXml, Map ctx, def messageLog) {

    HttpURLConnection conn
    try {
        URL url = new URL(ctx.sureTaxURL)
        conn = (HttpURLConnection) url.openConnection()
        conn.setRequestMethod('POST')
        conn.setDoOutput(true)
        String basicAuth = (ctx.sureTaxUsername + ':' + ctx.sureTaxPassword).bytes.encodeBase64().toString()
        conn.setRequestProperty('Authorization', "Basic ${basicAuth}")
        conn.setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')

        conn.outputStream.withWriter('UTF-8') { it << requestXml }

        int rc = conn.responseCode
        messageLog?.addAttachmentAsString('HTTP_Status', rc.toString(), 'text/plain')

        InputStream is = (rc >= 200 && rc < 300) ? conn.inputStream : conn.errorStream
        return is?.getText('UTF-8') ?: ''
    } finally {
        conn?.disconnect()
    }
}



// =================================================================================================
// 4. Response-Mapping  (SureTax -> SAP)
// =================================================================================================
/** Wandelt die SureTax-SOAP-Antwort ins SAP-Zielschema um. */
String mapResponse(String responseXml, Map ctx) {

    def rsp = new XmlSlurper().parseText(responseXml)
    int currDecInt  = safeParseInt(ctx.exchangeCurrencyDecimal, 2)
    BigDecimal scale= 10G ** currDecInt

    // Summe aller TaxAmounts für TAXAMOV --------------------------------------------------
    BigDecimal taxAmountOverall = 0G
    rsp.'**'.findAll { it.name() == 'TaxAmount' }.each { taxAmountOverall += safeParseBigDecimal(it.text()) }

    taxAmountOverall = taxAmountOverall * scale

    // Formatierung gemäss Anforderung
    String taxamovStr = formatSignedTrailing(taxAmountOverall)

    // Ergebnis-Items pro Group ------------------------------------------------------------
    List<Map> resultItems = []
    rsp.'**'.findAll { it.name() == 'Group' }.each { grp ->

        String itemNo = grp.LineNumber.text().trim()

        // Sammle Revenue- & TaxAmount-Summen der Group
        BigDecimal groupRev   = 0G
        BigDecimal groupTax   = 0G
        grp.TaxList.Tax.each {
            groupRev += safeParseBigDecimal(it.Revenue.text())
            groupTax += safeParseBigDecimal(it.TaxAmount.text())
        }

        // Hilfswerte aus erstem Tax-Eintrag
        def firstTax = grp.TaxList.Tax[0]
        int  taxCnt  = safeParseInt(firstTax.NumberOfTaxes.text(), grp.TaxList.Tax.size())
        int  grpCnt  = safeParseInt(firstTax.NumberOfGroups.text(), 1)
        int  cntGrpTax = (grpCnt == 0) ? 1 : (taxCnt / grpCnt)

        BigDecimal exemptAmount = (cntGrpTax == 0 ? 0G : (groupRev / cntGrpTax))
        BigDecimal percentTaxable = safeParseBigDecimal(firstTax.PercentTaxable.text())
        exemptAmount = exemptAmount - (exemptAmount * percentTaxable)
        exemptAmount = exemptAmount * scale

        Map row = [
                ITEM_NO : itemNo,
                EXAMT   : formatSignedTrailing(exemptAmount),
                TAXAMT  : formatSignedTrailing(groupTax * scale),
                EXCODE  : firstTax.ExemptCode.text().trim()
        ]
        resultItems << row
    }

    // Ziel-XML bauen ---------------------------------------------------------------------
    StringWriter sw = new StringWriter()
    MarkupBuilder mb = new MarkupBuilder(sw)

    mb.'TAX_CALCULATION_RECEIVE'('xmlns':'http://sap.com/xi/FotETaxUS') {
        'CALCULATION_RESULT_ITEM' {
            'TAXAMOV'(taxamovStr)
        }
        resultItems.each { row ->
            'RESULT_ITEM_JUR' {
                'ITEM_NO'(row.ITEM_NO)
                'EXAMT'(row.EXAMT)
                'TAXAMT'(row.TAXAMT)
                'EXCODE'(row.EXCODE)
            }
        }
    }
    return sw.toString()
}



// =================================================================================================
// 5. Logging-Helfer
// =================================================================================================
/** Fügt dem MPL ein Attachment hinzu; ignoriert Fehler im nicht-MPL-Kontext. */
void logStep(def messageLog, String name, String content) {
    try {
        messageLog?.addAttachmentAsString(name, content ?: '', 'text/xml')
    } catch (Exception ignore) { /* kein MPL verfügbar */ }
}



// =================================================================================================
// 6. Zentrales Error-Handling
// =================================================================================================
/** Anhängen des fehlerhaften Payloads & Werfen einer RuntimeException. */
void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Mapping-Script: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}



// =================================================================================================
// 7. Utility-Methoden
// =================================================================================================
/** Normalisiert negative Zahlen:  '500-' -> '-500'  |  '  ' -> '0'. */
private static String normalizeNegative(String v) {
    if (v == null) return '0'
    String t = v.trim()
    return (t.endsWith('-')) ? ('-' + t[0..-2]) : t
}

/** Gibt BigDecimal skaliert zurück; fehlerhafte Werte führen zu 0. */
private static BigDecimal parseScaled(String s, int dec) {
    BigDecimal bd = safeParseBigDecimal(s)
    if (dec == 0) return bd
    return bd.divide(10G ** dec, dec, BigDecimal.ROUND_HALF_EVEN)
}

/** Wandelt String sicher in BigDecimal; bei Fehler 0. */
private static BigDecimal safeParseBigDecimal(String s) {
    try {
        return new BigDecimal(normalizeNegative(s ?: '0'))
    } catch (Exception ex) {
        return 0G
    }
}

/** Wandelt String sicher in Integer; bei Fehler defaultVal. */
private static int safeParseInt(String s, int defaultVal) {
    try {
        return Integer.parseInt(s?.trim())
    } catch (Exception ex) {
        return defaultVal
    }
}

/** Entfernt überflüssige Nachkommastellen (z.B. 0.2000 -> 0.2). */
private static String trimTrailingZeros(BigDecimal bd) {
    return bd.stripTrailingZeros().toPlainString()
}

/** Formatiert Zahl so, dass negatives Vorzeichen hinten steht (-123 -> 123-). */
private static String formatSignedTrailing(BigDecimal bd) {
    if (bd == null) return ''
    String s = bd.setScale(0, BigDecimal.ROUND_HALF_EVEN).toPlainString().trim()
    return (s.startsWith('-')) ? (s.substring(1) + '-') : s
}