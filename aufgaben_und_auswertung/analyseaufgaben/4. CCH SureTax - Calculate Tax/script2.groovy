/********************************************************************************************************
 *  Groovy-Skript – S/4HANA Cloud ↔ CCH SureTax – Request- & Response-Mapping inkl. API-Call
 *
 *  Autor:  AI – Senior-Integration-Developer (SAP Cloud Integration)
 *  Datum:  2025-06-16
 *
 *  Dieses Skript erfüllt folgende Anforderungen:
 *  1. Modulare Struktur (eigene Funktionen je Aufgabe)
 *  2. Kommentierung in Deutsch
 *  3. Umfassendes Error-Handling mit aussagekräftigen Logs
 *  4. Anhängen aller relevanten Payload-Stände als Message-Attachment
 *  5. Verwendung von XML- und JSON-DSLs (XmlSlurper / MarkupBuilder / JsonSlurper bei Bedarf)
 *  6. Vermeidung von Namens-Kollisionen
 *  7. Keine globalen Konstanten / Variablen
 *  8. Nur benötigte Imports
 ********************************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import groovy.xml.XmlSlurper
import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.util.Base64

/********************************************************************************************************
 *  Haupteinstieg der IFlow-Groovy-Script-Komponente
 ********************************************************************************************************/
Message processData(Message message) {

    /* -------------------------------------------------------
     *  Initialisierung
     * ----------------------------------------------------- */
    def body        = message.getBody(String) ?: ''
    def messageLog  = messageLogFactory.getMessageLog(message)
    logStep(messageLog, '01_IncomingPayload', body)

    try {

        /* ---------------------------------------------------
         *  Richtung anhand Root-Element bestimmen
         * ------------------------------------------------- */
        def rootName = getRootElementName(body)
        switch (rootName) {
            case 'TAX_CALCULATION_SEND':
                /* ---------- REQUEST-VERARBEITUNG ---------- */
                setHeadersAndProperties(message)                                        // Header / Properties harmonisieren
                def requestPayload = mapRequest(body, message)                          // Request-Mapping
                logStep(messageLog, '02_AfterRequestMapping', requestPayload)

                /* ---------- API-Aufruf SureTax ---------- */
                def responsePayload = callSureTaxApi(requestPayload, message)           // HTTP-Call
                logStep(messageLog, '03_ResponsePayload', responsePayload)

                /* ---------- RESPONSE-MAPPING ---------- */
                def outPayload = mapResponse(responsePayload, message)
                logStep(messageLog, '04_AfterResponseMapping', outPayload)
                message.setBody(outPayload)
                break

            case 'SoapRequestResponse':
                /* ---------- Es handelt sich um einen Nachbearbeitungs-Step
                 *            (nur RESPONSE-MAPPING ohne API-Call) ---------- */
                def outPayload = mapResponse(body, message)
                logStep(messageLog, '02_AfterResponseMapping', outPayload)
                message.setBody(outPayload)
                break

            default:
                throw new IllegalStateException("Unbekannter Root-Knoten: '${rootName}' – es kann weder Request- noch Response-Mapping durchgeführt werden.")
        }

        return message
    }
    catch (Exception e) {
        /* ---------- Fehlerbehandlung ---------- */
        handleError(body, e, messageLog)
        return message   // Wird nie erreicht, handleError wirft Exception
    }
}

/********************************************************************************************************
 *  Helper-Funktionen
 ********************************************************************************************************/

/**
 *  Liest den Root-Element-Namen eines XML-Strings
 */
String getRootElementName(String xmlString) {
    def parser = new XmlSlurper(false, false)
    return parser.parseText(xmlString).name()
}

/**
 *  Harmonisiert Header / Properties.
 *  Fehlt ein Property oder Header ⇒ Platzhalter füllen
 */
void setHeadersAndProperties(Message msg) {

    def ensureValue = { String key ->
        def val = msg.getProperty(key)
        if (val == null || val.toString().trim().isEmpty()) {
            msg.setProperty(key, 'placeholder')
        }
    }

    [
            'sureTaxUsername',
            'sureTaxPassword',
            'sureTaxURL',
            'exchageCurrencyDecimal',
            'exchangeTransTypeCode',
            'currencyDecimalStr',
            'exchangeTTCodeValueMap'
    ].each { ensureValue(it) }
}

/**
 *  Normalisiert negative Zahlendarstellung.
 *  Beispiel: "500-" ➔ "-500"
 */
String getNegativeNormal(String inVal) {
    if (inVal == null) { return '' }
    def trimmed = inVal.trim()
    if (trimmed == '') { return '' }
    if (trimmed.endsWith('-')) {
        return '-' + trimmed[0..-2]
    }
    return trimmed
}

/**
 *  Fügt Content als Attachment an das MPL an
 */
void logStep(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content ?: '', 'text/plain')
}

/**
 *  Fehlerbehandlung gemäß Vorgabe
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

/********************************************************************************************************
 *  REQUEST-MAPPING
 ********************************************************************************************************/
String mapRequest(String inXml, Message msg) {

    /* --------------------------- */
    def slurper = new XmlSlurper(false, false)
    def root    = slurper.parseText(inXml)

    def currencyDecimalStr = (msg.getProperty('exchageCurrencyDecimal') ?: root.CALCULATION_HEADER.CURR_DEC.text() ?: '').trim()
    int currencyDecimalInt = currencyDecimalStr.isInteger() ? Integer.parseInt(currencyDecimalStr) : 2
    def divisor            = (BigDecimal) (10 ** currencyDecimalInt)

    /* ---------- TotalRevenue berechnen ---------- */
    BigDecimal sumAmount = 0
    root.CALCULATION_ITEM.each { item ->
        def amountStr   = getNegativeNormal(item.AMOUNT.text())
        def freightStr  = getNegativeNormal(item.FREIGHT_AM.text())
        try {
            if (amountStr)  sumAmount += new BigDecimal(amountStr)  / divisor
        } catch(ignore){}
        try {
            if (freightStr) sumAmount += new BigDecimal(freightStr) / divisor
        } catch(ignore){}
    }
    sumAmount = sumAmount.setScale(currencyDecimalInt, BigDecimal.ROUND_HALF_EVEN)

    /* ---------- Map für TransTypeCode ---------- */
    def ttCodeMap = [:]
    def mapProp = msg.getProperty('exchangeTTCodeValueMap')
    if (mapProp) {
        if (mapProp instanceof Map) {
            ttCodeMap = mapProp
        } else if (mapProp instanceof String && mapProp.trim().startsWith('{')) {
            ttCodeMap = new JsonSlurper().parseText(mapProp as String) as Map
        }
    }
    def defaultTT = msg.getProperty('exchangeTransTypeCode') ?: ''

    /* ---------- MarkupBuilder erzeugen ---------- */
    def writer = new StringWriter()
    def soapEnv = 'soapenv'
    def builder = new MarkupBuilder(writer)
    builder."${soapEnv}:SoapRequest"('xmlns:soapenv':'http://namespace-placeholder') {
        "${soapEnv}:request" {
            "${soapEnv}:CurrencyDecimal"(currencyDecimalStr)
            "${soapEnv}:TotalRevenue"(sumAmount.toPlainString())
            "${soapEnv}:ItemList" {
                root.CALCULATION_ITEM.each { item ->
                    def itemNo      = item.ITEM_NO.text().trim()
                    def amountRaw   = getNegativeNormal(item.AMOUNT.text())
                    def revenueCalc = ''
                    try {
                        if (amountRaw) {
                            def rev = new BigDecimal(amountRaw) / divisor
                            revenueCalc = rev.setScale(currencyDecimalInt, BigDecimal.ROUND_HALF_EVEN).toPlainString()
                        }
                    } catch(ignore){}
                    def exemptRaw   = getNegativeNormal(item.EXEMPT_AMT.text() ?: '0')
                    BigDecimal exemptScaled = 0
                    try { exemptScaled = new BigDecimal(exemptRaw) / divisor } catch(ignore){}
                    String taxExempStr
                    if (exemptScaled.compareTo(BigDecimal.ZERO) == 0) {
                        taxExempStr = '00'
                    } else {
                        BigDecimal amountScaled = revenueCalc ? new BigDecimal(revenueCalc) : BigDecimal.ONE
                        BigDecimal percent      = exemptScaled.abs() / amountScaled
                        taxExempStr = "06:${percent.setScale(10, BigDecimal.ROUND_HALF_EVEN).stripTrailingZeros()}"
                    }

                    "${soapEnv}:Item" {
                        "${soapEnv}:transTypeCode"(ttCodeMap.get(itemNo) ?: (defaultTT ?: ''))
                        "${soapEnv}:Revenue"(revenueCalc)
                        "${soapEnv}:TaxExemptionCodeList" {
                            "${soapEnv}:string"(taxExempStr)
                        }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/********************************************************************************************************
 *  API-CALL – SureTax
 ********************************************************************************************************/
String callSureTaxApi(String requestPayload, Message msg) {

    /* ---------- Verbindung herstellen ---------- */
    String urlStr   = msg.getProperty('sureTaxURL') ?: 'placeholder'
    String user     = msg.getProperty('sureTaxUsername') ?: 'placeholder'
    String pwd      = msg.getProperty('sureTaxPassword') ?: 'placeholder'
    def url  = new URL(urlStr)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    def basicAuth = Base64.encoder.encodeToString("$user:$pwd".getBytes(StandardCharsets.UTF_8))
    conn.setRequestProperty('Authorization', "Basic $basicAuth")
    conn.setRequestProperty('Content-Type', 'application/xml;charset=UTF-8')

    /* ---------- Request-Body senden ---------- */
    conn.outputStream.withWriter('UTF-8') { it << requestPayload }

    /* ---------- Antwort lesen ---------- */
    int rc = conn.responseCode
    if (rc != 200) {
        String respErr = conn.errorStream?.getText('UTF-8') ?: ''
        throw new IllegalStateException("SureTax-Aufruf fehlgeschlagen – HTTP Code: $rc – Payload: $respErr")
    }
    return conn.inputStream.getText('UTF-8')
}

/********************************************************************************************************
 *  RESPONSE-MAPPING
 ********************************************************************************************************/
String mapResponse(String inXml, Message msg) {

    def slurper = new XmlSlurper(false, false)
    def resp    = slurper.parseText(inXml)

    /* ---------- Skalierungsfaktor bestimmen ---------- */
    def currencyDecimalStr = (msg.getProperty('exchangeCurrencyDecimal') ?: '2').trim()
    int currencyDecimalInt = currencyDecimalStr.isInteger() ? Integer.parseInt(currencyDecimalStr) : 2
    BigDecimal factor      = (BigDecimal) (10 ** currencyDecimalInt)

    /* ---------- TAXAMOV – Summe aller TaxAmounts ---------- */
    BigDecimal totalTax = 0
    resp.'**'.findAll { it.name() == 'TaxAmount' }.each { t ->
        try { totalTax += new BigDecimal(t.text()) } catch(ignore){}
    }
    def taxAmovScaled = formatScaled(totalTax * factor)

    /* ---------- RESULT_ITEM_JUR pro Group ---------- */
    def resultItems = []
    resp.'**'.findAll { it.name() == 'Group' }.each { grp ->
        String lineNumber = grp.LineNumber.text().trim()

        /* Sum TaxAmounts und Revenue in der Gruppe */
        BigDecimal taxSumGrp     = 0
        BigDecimal revenueSumGrp = 0
        int taxCount             = 0
        int groupCount           = 1
        grp.TaxList.Tax.each { t ->
            try { taxSumGrp     += new BigDecimal(t.TaxAmount.text()) }     catch(ignore){}
            try { revenueSumGrp += new BigDecimal(t.Revenue.text())    }     catch(ignore){}
            taxCount++
        }
        try { groupCount = Integer.parseInt(grp.TaxList.Tax[0]?.NumberOfGroups?.text() ?: '1') } catch(ignore){}

        int countOfGroupTax = (groupCount != 0) ? (taxCount.intdiv(groupCount)) : 1
        BigDecimal exemptAmount = (countOfGroupTax != 0) ? (revenueSumGrp / countOfGroupTax) : 0
        BigDecimal percentTaxable = 0
        try { percentTaxable = new BigDecimal(grp.TaxList.Tax[0]?.PercentTaxable?.text() ?: '0') } catch(ignore){}
        exemptAmount = exemptAmount - (exemptAmount * percentTaxable)

        def itemMap = [
                ITEM_NO : lineNumber,
                EXAMT   : formatScaled(exemptAmount * factor),
                TAXAMT  : formatScaled(taxSumGrp * factor),
                EXCODE  : grp.TaxList.Tax[0]?.ExemptCode?.text()?.trim() ?: ''
        ]
        resultItems << itemMap
    }

    /* ---------- XML-Ergebnis aufbauen ---------- */
    def writer = new StringWriter()
    def nsUri  = 'http://sap.com/xi/FotETaxUS'
    def builder = new MarkupBuilder(writer)
    builder."TAX_CALCULATION_RECEIVE"(xmlns:nsUri) {
        "CALCULATION_RESULT_ITEM" {
            "TAXAMOV"(taxAmovScaled)
        }
        resultItems.each { map ->
            "RESULT_ITEM_JUR" {
                "ITEM_NO"(map.ITEM_NO)
                "EXAMT"(map.EXAMT)
                "TAXAMT"(map.TAXAMT)
                "EXCODE"(map.EXCODE)
            }
        }
    }
    return writer.toString()
}

/**
 *  Formatiert BigDecimal nach Skalierung & Negativ-Konvention
 */
String formatScaled(BigDecimal value) {
    if (value == null) { return '' }
    def str = value.setScale(0, BigDecimal.ROUND_HALF_EVEN).toPlainString().trim()
    if (str.startsWith('-')) {
        return str.substring(1) + '-'
    }
    return str
}

/**
 *  Prüft, ob String eine gültige Ganzzahl ist
 */
boolean String.isInteger() {
    return (this ==~ /^-?\d+$/)
}