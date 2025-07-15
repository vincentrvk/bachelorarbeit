/****************************************************************************************
 *  Groovy-Skript:  S4-CCH SureTax Integration – Request- und Response-Mapping
 *  Autor        :  ChatGPT (Senior-Developer SAP CI)
 *  Version      :  1.0
 *
 *  Beschreibung :
 *  ­-------------
 *  Dieses Skript liest den eingehenden S/4HANA-Payload, erzeugt daraus den
 *  SureTax-Request (SOAP-Body), ruft den Web-Service auf, verarbeitet die Antwort
 *  und gibt das Ergebnis im SAP-Zielschema zurück.
 *
 *  Implementierte Anforderungen (Kurzfassung)
 *  ­-----------------------------------------
 *  1. Modularer Aufbau (jede Hauptaufgabe hat eine separate Funktion)
 *  2. Vollständige deutschsprachige Kommentare
 *  3. Umfassendes Error-Handling inkl. Attachment des fehlerhaften Payloads
 *  4. Logging aller relevanten Schritte (ebenfalls als Attachment)
 *  5. Nutzung von XML-Parsern (XmlSlurper/MarkupBuilder) & BigDecimal-Arithmetik
 *  6. Keine globalen Konstanten, keine ungenutzten Imports
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.math.RoundingMode
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets

// === Einstiegspunkt ================================================================
Message processData(Message message) {

    // MessageLog instanziieren
    def messageLog = messageLogFactory.getMessageLog(message)

    try {

        // 1) Eingehenden Payload sichern
        def incomingBody = message.getBody(String) ?: ""
        logStep(messageLog, "1-IncomingPayload", incomingBody)

        // 2) Header & Properties einlesen / anreichern
        def ctx = buildContext(message)

        // 3) Request-Mapping durchführen
        def sureTaxRequestXml = mapRequest(incomingBody, ctx)
        logStep(messageLog, "2-AfterRequestMapping", sureTaxRequestXml)

        // 4) Aufruf SureTax-Service
        def sureTaxResponseXml = callSureTax(sureTaxRequestXml, ctx, messageLog)
        logStep(messageLog, "3-SureTaxResponseRaw", sureTaxResponseXml)

        // 5) Response-Mapping durchführen
        def s4ResponseXml = mapResponse(sureTaxResponseXml, ctx)
        logStep(messageLog, "4-AfterResponseMapping", s4ResponseXml)

        // 6) Ergebnis ins Message-Body schreiben
        message.setBody(s4ResponseXml)

    } catch (Exception e) {
        handleError(incomingBody ?: message.getBody(String), e, messageLog)
    }

    return message
}

/* =====================================================================================
 *  buildContext
 *  Liest benötigte Header-/Property-Werte ein oder vergibt "placeholder"
 * ===================================================================================*/
private static Map buildContext(Message msg) {

    Map ctx = [:]

    // Properties (können im IFlow gesetzt sein)
    ctx.sureTaxUsername        = msg.getProperty('sureTaxUsername')        ?: 'placeholder'
    ctx.sureTaxPassword        = msg.getProperty('sureTaxPassword')        ?: 'placeholder'
    ctx.sureTaxURL             = msg.getProperty('sureTaxURL')             ?: 'placeholder'
    ctx.exchangeCurrencyDecimal= msg.getProperty('exchageCurrencyDecimal') ?: null          // kann null sein
    ctx.exchangeTransTypeCode  = msg.getProperty('exchangeTransTypeCode')  ?: null
    ctx.exchangeTTCodeValueMap = msg.getProperty('exchangeTTCodeValueMap') ?: [:]

    return ctx
}

/* =====================================================================================
 *  mapRequest
 *  Wandelt S/4HANA-XML in das SureTax-SOAP-Requestschema
 * ===================================================================================*/
private static String mapRequest(String sourceXml, Map ctx) {

    def source      = new XmlSlurper().parseText(sourceXml)
    String currDec  = source.CALCULATION_HEADER.CURR_DEC.text().trim()
    int    currDecI = safeInt(currDec, 2)
    BigDecimal divisor = BigDecimal.TEN.pow(currDecI)

    // -- TotalRevenue berechnen -------------------------------------------------------
    def amountList  = (source.CALCULATION_ITEM.AMOUNT*.text() ?: [])
    def freightList = (source.CALCULATION_ITEM.FREIGHT_AM*.text() ?: [])
    BigDecimal totalRevenue = (amountList + freightList)
            .collect { toBigDecimalSafe(getNegativeNormal(it), divisor) }
            .inject(BigDecimal.ZERO, BigDecimal::add)
            .setScale(currDecI, RoundingMode.HALF_EVEN)

    // -- Item-Liste aufbauen -----------------------------------------------------------
    def items = source.CALCULATION_ITEM.collect { item ->

        String itemNo  = item.ITEM_NO.text().trim()
        String amount  = item.AMOUNT.text()
        String exempt  = item.EXEMPT_AMT.text()

        // a) transTypeCode
        String transType = ''
        if (ctx.exchangeTTCodeValueMap[itemNo]) {
            transType = ctx.exchangeTTCodeValueMap[itemNo]
        } else if (ctx.exchangeTransTypeCode) {
            transType = ctx.exchangeTransTypeCode
        }

        // b) Revenue
        BigDecimal rev = toBigDecimalSafe(getNegativeNormal(amount), divisor)
                           .setScale(currDecI, RoundingMode.HALF_EVEN)

        // c) TaxExemptionCodeList
        BigDecimal exemptVal = toBigDecimalSafe(getNegativeNormal(exempt ?: "0"), divisor)
        String taxExString
        if (exemptVal == BigDecimal.ZERO) {
            taxExString = "00"
        } else {
            BigDecimal percentage = (exemptVal.abs() / rev).setScale(4, RoundingMode.HALF_EVEN)
            taxExString = "06:${percentage.stripTrailingZeros().toPlainString()}"
        }

        // Rückgabe Map pro Item
        [
            transTypeCode : transType,
            revenue       : rev.toPlainString(),
            taxExStr      : taxExString
        ]
    }

    // -- XML Result mit MarkupBuilder --------------------------------------------------
    def writer = new StringWriter()
    def xml    = new MarkupBuilder(writer)
    xml.'soapenv:SoapRequest'('xmlns:soapenv': 'http://namespace-placeholder') {
        'soapenv:request' {
            'soapenv:CurrencyDecimal'(currDec)
            'soapenv:TotalRevenue'(totalRevenue.toPlainString())
            'soapenv:ItemList' {
                items.each { i ->
                    'soapenv:Item' {
                        'soapenv:transTypeCode'(i.transTypeCode)
                        'soapenv:Revenue'(i.revenue)
                        'soapenv:TaxExemptionCodeList' {
                            'soapenv:string'(i.taxExStr)
                        }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/* =====================================================================================
 *  callSureTax
 *  Ruft den Remote-Service via HTTP POST auf und liefert die Antwort als String zurück
 *  Hinweis: Für CPI-Produktivbetrieb besser den HTTP-Adapter nutzen – dies dient
 *  nur als Beispiel für ein reines Groovy-Skript.
 * ===================================================================================*/
private static String callSureTax(String payload, Map ctx, def messageLog) {

    if (ctx.sureTaxURL == 'placeholder') {
        // Demo-Funktion: Wenn keine echte URL vorhanden ist, Mock-Response zurückgeben
        messageLog?.addAttachmentAsString("Info", "Keine SureTax-URL vorhanden – Mock-Antwort generiert", "text/plain")
        return "<SoapRequestResponse/>"
    }

    URL url = new URL(ctx.sureTaxURL)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod("POST")
    String basicAuth = "${ctx.sureTaxUsername}:${ctx.sureTaxPassword}".bytes.encodeBase64().toString()
    conn.setRequestProperty("Authorization", "Basic $basicAuth")
    conn.setRequestProperty("Content-Type", "text/xml; charset=UTF-8")
    conn.setDoOutput(true)

    conn.outputStream.withWriter("UTF-8") { it << payload }
    int rc = conn.responseCode
    String responseBody = conn.inputStream.getText(StandardCharsets.UTF_8.name())

    messageLog?.addAttachmentAsString("HTTP-Status", rc.toString(), "text/plain")
    return responseBody
}

/* =====================================================================================
 *  mapResponse
 *  Wandelt die SureTax-Antwort in das SAP-Zielschema
 * ===================================================================================*/
private static String mapResponse(String respXml, Map ctx) {

    def resp        = new XmlSlurper().parseText(respXml)
    int currDecI    = safeInt(ctx.exchangeCurrencyDecimal, 2)
    BigDecimal mult = BigDecimal.TEN.pow(currDecI)

    // --- TAXAMOV (Summe aller TaxAmount) --------------------------------------------
    BigDecimal taxSum = resp.'**'.findAll { it.name() == 'TaxAmount' }*.text()
                       .collect { new BigDecimal(it ?: "0") }
                       .inject(BigDecimal.ZERO, BigDecimal::add)

    BigDecimal taxMovScaled = taxSum * mult
    String taxMovStr        = formatNeg(taxMovScaled)

    // --- RESULT_ITEM_JUR Liste ------------------------------------------------------
    def resultItems = []
    resp.'**'.findAll { it.name() == 'Group' }.each { grp ->
        String lineNo = grp.LineNumber.text()
        grp.TaxList.Tax.each { taxNode ->
            BigDecimal revenue       = new BigDecimal(taxNode.Revenue.text() ?: "0")
            int numberOfTaxes        = safeInt(taxNode.NumberOfTaxes.text(), 1)
            int numberOfGroups       = safeInt(taxNode.NumberOfGroups.text(), 1)
            int countOfGroupTax      = numberOfTaxes / numberOfGroups
            BigDecimal exemptAmount  = (revenue / countOfGroupTax)              // Aufteilung
                                         * (1 - new BigDecimal(taxNode.PercentTaxable.text() ?: "0"))
            BigDecimal exAmtScaled   = exemptAmount * mult
            BigDecimal taxAmtScaled  = new BigDecimal(taxNode.TaxAmount.text() ?: "0") * mult

            resultItems << [
                ITEM_NO : lineNo?.padLeft(6, '0'),
                EXAMT   : formatNeg(exAmtScaled),
                TAXAMT  : formatNeg(taxAmtScaled),
                EXCODE  : taxNode.ExemptCode.text() ?: ''
            ]
        }
    }

    // --- Aufbau Ziel-XML ------------------------------------------------------------
    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)
    xml.'TAX_CALCULATION_RECEIVE'('xmlns': 'http://sap.com/xi/FotETaxUS') {
        'CALCULATION_RESULT_ITEM' {
            'TAXAMOV'(taxMovStr)
        }
        resultItems.each { itMap ->
            'RESULT_ITEM_JUR' {
                'ITEM_NO'(itMap.ITEM_NO)
                'EXAMT'(itMap.EXAMT)
                'TAXAMT'(itMap.TAXAMT)
                'EXCODE'(itMap.EXCODE)
            }
        }
    }
    return sw.toString()
}

/* =====================================================================================
 *  Hilfsfunktionen
 * ===================================================================================*/

/* getNegativeNormal
 * Wandelt Formate wie "500-" in "-500" */
private static String getNegativeNormal(String val) {
    if (val == null) return ""
    val = val.trim()
    return val.endsWith('-') && !val.startsWith('-') ? "-${val[0..-2]}" : val
}

/* toBigDecimalSafe
 * Konvertiert String zu BigDecimal, teilt durch divisor, fehlerhafte Werte = 0 */
private static BigDecimal toBigDecimalSafe(String strVal, BigDecimal divisor) {
    try {
        if (!strVal) return BigDecimal.ZERO
        return new BigDecimal(strVal).divide(divisor)
    } catch (Exception ignored) {
        return BigDecimal.ZERO
    }
}

/* safeInt
 * Liefert Integer-Wert oder Fallback */
private static int safeInt(Object val, int defaultVal) {
    try { return Integer.parseInt(val as String) } catch (Exception e) { return defaultVal }
}

/* formatNeg
 * Entfernt Leerzeichen, hängt minus hinten an, falls benötigt */
private static String formatNeg(BigDecimal num) {
    if (num == null) return ""
    String s = num.stripTrailingZeros().toPlainString().trim()
    return s.startsWith('-') ? "${s.substring(1)}-" : s
}

/* =====================================================================================
 *  logStep
 *  Fügt einen String-Anhang für Monitoring-Zwecke hinzu
 * ===================================================================================*/
private static void logStep(def messageLog, String name, String content) {
    try {
        messageLog?.addAttachmentAsString(name, content, "text/xml")
    } catch (Exception ignored) {
        // Logging darf das Haupt-Processing nicht stören
    }
}

/* =====================================================================================
 *  handleError
 *  Zentrales Error-Handling inkl. Attachment des fehlerhaften Payloads
 * ===================================================================================*/
private static void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "", "text/xml")
    String errorMsg = "Fehler im SureTax-Integrationsskript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}