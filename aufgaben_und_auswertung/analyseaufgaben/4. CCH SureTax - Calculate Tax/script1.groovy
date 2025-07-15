/***************************************************************************************************
 *  Groovy-Skript – End-2-End SureTax-Integration (Request- u. Response-Mapping incl. HTTP-Call)
 *  Autor: Senior-Developer SAP Cloud Integration
 *
 *  WICHTIG:
 *  1. Jede Funktion ist separat gekapselt, dokumentiert und enthält Error-Handling.
 *  2. Logging wird über Message-Attachments realisiert (Schritte 1-4).
 *  3. Bei jeder Exception wird der Ursprungs-Payload als Attachment angehängt und Exception geworfen.
 ***************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import java.math.BigDecimal
import java.net.HttpURLConnection
import java.net.URL
import java.util.Base64
import groovy.xml.MarkupBuilder

// === Haupteinstieg ===============================================================================

Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)          // CPI-Logging
    def originalBody = message.getBody(String) ?: ""                   // Ursprungs-Payload

    try {
        // Schritt 1: Eingangspayload loggen
        logAttachment(messageLog, '01-InputPayload', originalBody)

        // Schritt 2: Variablen aus Properties / Header füllen
        def ctx = readContextVariables(message)

        // Schritt 3: Request-Mapping
        String requestXml = buildSureTaxRequest(originalBody, ctx)
        logAttachment(messageLog, '02-RequestMapping', requestXml)

        // Schritt 4: HTTP-Call zu SureTax
        String sureTaxResponse = callSureTax(requestXml, ctx)
        logAttachment(messageLog, '03-SureTaxResponseRaw', sureTaxResponse)

        // Schritt 5: Response-Mapping
        String outputXml = buildS4Response(sureTaxResponse, ctx)
        logAttachment(messageLog, '04-ResponseMapping', outputXml)

        // Schritt 6: Ergebnis in Message setzen
        message.setBody(outputXml)
        return message

    } catch (Exception e) {
        handleError(originalBody, e, messageLog)                       // zentrale Fehlerbehandlung
    }
}

/***************************************************************************************************
 *  readContextVariables:
 *  Liest benötigte Werte aus Headern / Properties oder setzt "placeholder".
 ***************************************************************************************************/
private Map readContextVariables(Message message) {
    Map<String, Object> ctx = [:]

    // Properties
    ctx.sureTaxUsername          = (message.getProperty('sureTaxUsername')       ?: 'placeholder') as String
    ctx.sureTaxPassword          = (message.getProperty('sureTaxPassword')       ?: 'placeholder') as String
    ctx.sureTaxURL               = (message.getProperty('sureTaxURL')            ?: 'placeholder') as String
    ctx.exchangeCurrencyDecimal  = (message.getProperty('exchageCurrencyDecimal')?: '2')          as String
    ctx.exchangeTransTypeCode    = (message.getProperty('exchangeTransTypeCode') ?: '')           as String
    ctx.exchangeTTCodeValueMap   = message.getProperty('exchangeTTCodeValueMap') ?: [:]

    // Header (Beispiel – kann je nach IFlow erweitert werden)
    ctx.currencyDecimalStrHeader = message.getHeader('CURR_DEC', String.class)   ?: ''

    return ctx
}

/***************************************************************************************************
 *  buildSureTaxRequest:
 *  Erstellt SOAP-Request an SureTax gem. Mapping-Regeln.
 ***************************************************************************************************/
private String buildSureTaxRequest(String inputXml, Map ctx) {

    def input = new XmlSlurper().parseText(inputXml)

    // CurrencyDecimal
    String currencyDecStr = (input.CALCULATION_HEADER.CURR_DEC.text() ?: '').trim()
    int currencyDecInt    = currencyDecStr.isInteger() ? currencyDecStr.toInteger() : 2
    BigDecimal decFactor  = BigDecimal.TEN.pow(currencyDecInt)

    // TotalRevenue berechnen
    BigDecimal totalRevenue = 0
    input.CALCULATION_ITEM.each { item ->
        totalRevenue += parseAmount(item.AMOUNT.text(), decFactor)
        totalRevenue += parseAmount(item.FREIGHT_AM.text(), decFactor)
    }
    totalRevenue = totalRevenue.setScale(currencyDecInt, BigDecimal.ROUND_HALF_EVEN)

    // ItemList erzeugen
    def writer = new StringWriter()
    def xml    = new MarkupBuilder(writer)
    xml.'soapenv:SoapRequest'('xmlns:soapenv':'http://namespace-placeholder') {
        'soapenv:request' {
            'soapenv:CurrencyDecimal'(currencyDecStr)
            'soapenv:TotalRevenue'(totalRevenue.toPlainString())
            'soapenv:ItemList' {
                input.CALCULATION_ITEM.each { item ->
                    'soapenv:Item' {
                        // transTypeCode
                        String itemNo = item.ITEM_NO.text()
                        String transTypeCode = ''
                        if (ctx.exchangeTTCodeValueMap instanceof Map && ctx.exchangeTTCodeValueMap[itemNo])
                            transTypeCode = ctx.exchangeTTCodeValueMap[itemNo]
                        else if (ctx.exchangeTransTypeCode)
                            transTypeCode = ctx.exchangeTransTypeCode
                        'soapenv:transTypeCode'(transTypeCode)

                        // Revenue
                        BigDecimal revenue = parseAmount(item.AMOUNT.text(), decFactor)
                        'soapenv:Revenue'(revenue.setScale(currencyDecInt, BigDecimal.ROUND_HALF_EVEN).toPlainString())

                        // TaxExemptionCodeList
                        'soapenv:TaxExemptionCodeList' {
                            'soapenv:string'(buildExemptString(item, decFactor, revenue))
                        }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/***************************************************************************************************
 *  callSureTax:
 *  Führt HTTP-POST gegen SureTax aus (Basic-Auth) und liefert Response-XML zurück.
 ***************************************************************************************************/
private String callSureTax(String requestXml, Map ctx) {
    HttpURLConnection conn = null
    try {
        URL url = new URL(ctx.sureTaxURL)
        conn = (HttpURLConnection) url.openConnection()
        conn.setRequestMethod('POST')
        conn.setDoOutput(true)
        String auth = "${ctx.sureTaxUsername}:${ctx.sureTaxPassword}"
        conn.setRequestProperty('Authorization', 'Basic ' + Base64.getEncoder().encodeToString(auth.getBytes('UTF-8')))
        conn.getOutputStream().withWriter('UTF-8') { it << requestXml }

        int rc = conn.getResponseCode()
        if (rc != 200) {
            throw new RuntimeException("SureTax HTTP-Error, ResponseCode = $rc")
        }
        return conn.getInputStream().getText('UTF-8')

    } finally {
        conn?.disconnect()
    }
}

/***************************************************************************************************
 *  buildS4Response:
 *  Wandelt SureTax-Response in S/4HANA-Format um.
 ***************************************************************************************************/
private String buildS4Response(String responseXml, Map ctx) {

    def response = new XmlSlurper().parseText(responseXml)

    int currencyDecInt = (ctx.exchangeCurrencyDecimal?.isInteger() ? ctx.exchangeCurrencyDecimal.toInteger() : 2)
    BigDecimal factor  = BigDecimal.TEN.pow(currencyDecInt)

    // TAXAMOV – Summe aller TaxAmount
    BigDecimal taxAmountSum = 0
    response.'**'.findAll { it.name() == 'TaxAmount' }.each {
        taxAmountSum += new BigDecimal(it.text().trim() ?: '0')
    }
    String taxAmov = scaleAndFormat(taxAmountSum * factor)

    // Ergebnis-XML bauen
    def writer = new StringWriter()
    def xmlOut = new MarkupBuilder(writer)
    xmlOut.'TAX_CALCULATION_RECEIVE'('xmlns':'http://sap.com/xi/FotETaxUS') {
        'CALCULATION_RESULT_ITEM' {
            'TAXAMOV'(taxAmov)
        }

        // RESULT_ITEM_JUR
        response.'**'.findAll { it.name() == 'Group' }.each { grp ->
            BigDecimal revenueSum = 0
            grp.TaxList.Tax.each { revenueSum += new BigDecimal(it.Revenue.text() ?: '0') }

            grp.TaxList.Tax.each { tax ->
                // ExemptAmount
                int groupCnt = (tax.NumberOfGroups.text() ?: '1').toInteger()
                int taxCnt   = (tax.NumberOfTaxes.text() ?: '1').toInteger()
                int cntPerGrp = (groupCnt != 0) ? (taxCnt / groupCnt) : 1
                BigDecimal exemptAmount = revenueSum / cntPerGrp
                BigDecimal percentTaxable = new BigDecimal(tax.PercentTaxable.text() ?: '0')
                BigDecimal examtCalc = exemptAmount - (exemptAmount * percentTaxable)
                String examt = scaleAndFormat(examtCalc * factor)

                // TAXAMT
                BigDecimal taxAmt = new BigDecimal(tax.TaxAmount.text() ?: '0')
                String taxamtStr  = scaleAndFormat(taxAmt * factor)

                // ITEM-Block schreiben
                'RESULT_ITEM_JUR' {
                    'ITEM_NO'(grp.LineNumber.text())
                    'EXAMT'(examt)
                    'TAXAMT'(taxamtStr)
                    'EXCODE'(tax.ExemptCode.text())
                }
            }
        }
    }
    return writer.toString()
}

/***************************************************************************************************
 *  Hilfsfunktionen
 ***************************************************************************************************/

// Negative Werte im Format "500-" → "-500"
private String normalizeNegative(String value) {
    if (!value) return ''
    value = value.trim()
    return value.endsWith('-') ? ('-' + value[0..-2]) : value
}

// Betrag als BigDecimal durch Dezimal-Faktor teilen
private BigDecimal parseAmount(String txt, BigDecimal factor) {
    try {
        txt = normalizeNegative(txt)
        if (!txt) return 0
        return new BigDecimal(txt).divide(factor)
    } catch (Exception ignore) {
        return 0
    }
}

// Exempt-String gem. Regelwerk erstellen
private String buildExemptString(def item, BigDecimal factor, BigDecimal amount) {
    BigDecimal exempt = parseAmount(item.EXEMPT_AMT.text(), factor)
    if (exempt == 0) return '00'
    BigDecimal percent = exempt.abs() / (amount == 0 ? 1 : amount.abs())
    return "06:${percent.stripTrailingZeros().toPlainString()}"
}

// Skaliert Wert und formatiert Negativzeichen ans Ende
private String scaleAndFormat(BigDecimal val) {
    if (val == null) return ''
    String str = val.stripTrailingZeros().toPlainString().trim()
    return str.startsWith('-') ? (str.substring(1) + '-') : str
}

// Attachment-Logging
private void logAttachment(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content, 'text/xml')
}

// Fehlerbehandlung
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}