/****************************************************************************************
 *  Groovy-Skript:  S4HANA → CCH SureTax → S4HANA
 *  Autor        :  ChatGPT (Senior-Integrationsentwickler)
 *  Version      :  1.0
 *
 *  Dieses Skript führt alle erforderlichen Schritte für die Steuer-Berechnung mit
 *  CCH SureTax in einer SAP Cloud Integration (IFlow) aus:
 *   1. Sammeln und Vorbelegen von Headern & Properties
 *   2. Request-Mapping (S/4HANA → SureTax)
 *   3. Aufruf der SureTax-SOAP-API
 *   4. Response-Mapping (SureTax → S/4HANA)
 *   5. Logging aller relevanten Zwischenschritte
 *   6. Zentrales, einheitliches Error-Handling
 *
 *  Hinweis:
 *  Alle Funktionen sind modular aufgebaut und enthalten ausführliche deutschsprachige
 *  Kommentare.  Fehlersituationen werden mit einer RuntimeException beendet, wobei der
 *  ursprüngliche Payload zusätzlich als Message-Attachment angehängt wird.
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import java.nio.charset.StandardCharsets
import java.math.BigDecimal
import javax.xml.parsers.DocumentBuilderFactory
import groovy.xml.MarkupBuilder
import groovy.xml.XmlUtil

// =======================================================================================
//  Haupteinstiegspunkt
// =======================================================================================
Message processData(Message message) {

    // 1. Sammeln / Vorbelegen der Header & Properties
    def ctx = initContext(message)

    // 2. Logging: Eingehender Payload
    addLogAttachment(message, '01-IncomingPayload', ctx.body)

    // 3. Request-Mapping
    def sureTaxRequest = mapRequest(ctx)
    addLogAttachment(message, '02-AfterRequestMapping', sureTaxRequest)

    // 4. Aufruf der SureTax-API
    def sureTaxResponse = callSureTax(ctx, sureTaxRequest)
    addLogAttachment(message, '03-RawSureTaxResponse', sureTaxResponse)

    // 5. Response-Mapping
    def s4Response = mapResponse(ctx, sureTaxResponse)
    addLogAttachment(message, '04-AfterResponseMapping', s4Response)

    // 6. Rückgabe des gemappten Response-Payloads
    message.setBody(s4Response)
    return message
}

// =======================================================================================
//  Modul 1: Kontext (Header / Properties) initialisieren
// =======================================================================================
/**
 *  Liest alle relevanten Header & Properties aus dem Message-Objekt, legt Default-Werte
 *  fest und fasst sie gemeinsam mit dem Body in einer Map zusammen.
 */
def Map initContext(Message message) {
    try {
        Map ctx = [:]
        ctx.body                    = message.getBody(String) ?: ''
        ctx.messageLog              = messageLogFactory.getMessageLog(message)

        // Properties (Default: "placeholder" wenn nicht vorhanden)
        ctx.sureTaxUsername         = getPropOrDefault(message, 'sureTaxUsername')
        ctx.sureTaxPassword         = getPropOrDefault(message, 'sureTaxPassword')
        ctx.sureTaxURL              = getPropOrDefault(message, 'sureTaxURL')
        ctx.exchangeCurrencyDecimal = getPropOrDefault(message, 'exchageCurrencyDecimal', '2')
        ctx.exchangeTransTypeCode   = getPropOrDefault(message, 'exchangeTransTypeCode', '')
        ctx.exchangeTTCodeValueMap  = getPropOrDefault(message, 'exchangeTTCodeValueMap', [:])

        return ctx
    } catch (Exception e) {
        handleError(message, e, 'Fehler beim Initialisieren des Kontextes')
    }
}

/** Liefert Property-Wert oder Default zurück */
def getPropOrDefault(Message message, String key, def defaultVal = 'placeholder') {
    def val = message.getProperty(key)
    return (val == null || val.toString().trim().isEmpty()) ? defaultVal : val
}

// =======================================================================================
//  Modul 2: Request-Mapping (S/4 → SureTax)
// =======================================================================================
/**
 *  Erzeugt das SOAP-Request-XML für SureTax.
 */
def String mapRequest(Map ctx) {
    try {
        def xml = new XmlSlurper().parseText(ctx.body)

        /* -------------------------------------------------------------------------
         *  Currency-Decimal ermitteln
         * ---------------------------------------------------------------------- */
        String currDecStr      = xml.CALCULATION_HEADER.CURR_DEC.text().trim()
        int    currencyDecInt  = currDecStr.isInteger() ? currDecStr.toInteger() : 2
        BigDecimal decDivider  = 10G.pow(currencyDecInt)

        /* -------------------------------------------------------------------------
         *  Total Revenue berechnen
         * ---------------------------------------------------------------------- */
        BigDecimal totalRev = 0G
        xml.CALCULATION_ITEM.each { item ->
            totalRev += (
                strToDecimal(getNegativeNormal(item.AMOUNT.text()), decDivider) +
                strToDecimal(getNegativeNormal(item.FREIGHT_AM.text()), decDivider)
            )
        }

        /* -------------------------------------------------------------------------
         *  Items abbilden
         * ---------------------------------------------------------------------- */
        def itemList = xml.CALCULATION_ITEM.collect { item ->
            // transTypeCode-Ermittlung über Map
            String itemNo = item.ITEM_NO.text()
            String transType = ctx.exchangeTTCodeValueMap[itemNo] ?:
                               ctx.exchangeTransTypeCode ?: ''

            // Revenue des Items
            BigDecimal revenue = strToDecimal(getNegativeNormal(item.AMOUNT.text()), decDivider)

            // Tax-Exemption-String
            BigDecimal exemptAmt = strToDecimal(getNegativeNormal(
                                    item.EXEMPT_AMT.text() ?: '0'), decDivider)

            String exemptionStr = buildExemptionString(exemptAmt, revenue)

            return [transType: transType,
                    revenue   : revenue.setScale(currencyDecInt, BigDecimal.ROUND_HALF_EVEN)
                                       .toPlainString(),
                    exemption : exemptionStr]
        }

        /* -------------------------------------------------------------------------
         *  XML per MarkupBuilder generieren
         * ---------------------------------------------------------------------- */
        def sw = new StringWriter()
        def builder = new MarkupBuilder(sw)
        builder.'soapenv:SoapRequest'('xmlns:soapenv': 'http://namespace-placeholder') {
            'soapenv:request' {
                'soapenv:CurrencyDecimal'(currDecStr)
                'soapenv:TotalRevenue'(totalRev.setScale(currencyDecInt,
                                                  BigDecimal.ROUND_HALF_EVEN).toPlainString())
                'soapenv:ItemList' {
                    itemList.each { itm ->
                        'soapenv:Item' {
                            'soapenv:transTypeCode'(itm.transType)
                            'soapenv:Revenue'(itm.revenue)
                            'soapenv:TaxExemptionCodeList' {
                                'soapenv:string'(itm.exemption)
                            }
                        }
                    }
                }
            }
        }
        return XmlUtil.serialize(sw.toString())
    } catch (Exception e) {
        handleError(ctx.messageLog, e, 'Fehler im Request-Mapping')
    }
}

/**  Normalisiert negative Werte (z. B. "500-" → "-500"). */
def String getNegativeNormal(String value) {
    if (!value) return ''
    value = value.trim()
    return value.endsWith('-') ? "-${value[0..-2]}" : value
}

/**  Wandelt String in BigDecimal und teilt durch Dezimal-Divider. */
def BigDecimal strToDecimal(String str, BigDecimal divider) {
    if (!str?.isNumber()) return 0G
    return new BigDecimal(str) / divider
}

/** Erzeugt den Exemption-String laut Vorgabe */
def String buildExemptionString(BigDecimal exempt, BigDecimal amount) {
    if (amount == 0G) {
        return '00'
    }
    BigDecimal percent = exempt.abs() / amount.abs()
    if (percent == 0G) {
        return '00'
    }
    return "06:${percent.toPlainString()}"
}

// =======================================================================================
//  Modul 3: Aufruf CCH SureTax
// =======================================================================================
/**
 *  Ruft den SureTax-Service via HTTP-POST (Basic Auth) auf und gibt den Response-Body
 *  als String zurück.
 */
def String callSureTax(Map ctx, String requestXml) {
    HttpURLConnection conn
    try {
        URL url = new URL(ctx.sureTaxURL)
        conn = url.openConnection() as HttpURLConnection
        conn.with {
            requestMethod       = 'POST'
            doOutput            = true
            setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')
            String basicAuth    = "${ctx.sureTaxUsername}:${ctx.sureTaxPassword}"
                                     .bytes.encodeBase64().toString()
            setRequestProperty('Authorization', "Basic $basicAuth")
            outputStream.withWriter('UTF-8') { it << requestXml }
        }

        int rc = conn.responseCode
        if (rc != 200) {
            throw new RuntimeException("Unerwarteter HTTP-Status von SureTax: $rc")
        }
        return conn.inputStream.getText('UTF-8')
    } catch (Exception e) {
        handleError(ctx.messageLog, e, 'Fehler beim Aufruf von SureTax')
    } finally {
        conn?.disconnect()
    }
}

// =======================================================================================
//  Modul 4: Response-Mapping (SureTax → S/4)
// =======================================================================================
/**
 *  Wandelt das SOAP-Response-XML von SureTax in das erwartete S/4-Zielschema um.
 */
def String mapResponse(Map ctx, String sureTaxResp) {
    try {
        def resp = new XmlSlurper().parseText(sureTaxResp)

        /* -------------------------------------------------------------------------
         *  Vorbereitende Werte
         * ---------------------------------------------------------------------- */
        int currencyDecInt    = ctx.exchangeCurrencyDecimal.isInteger() ?
                                ctx.exchangeCurrencyDecimal.toInteger() : 2
        BigDecimal multiplier = 10G.pow(currencyDecInt)

        /* -------------------------------------------------------------------------
         *  Gesamt-TaxAmount (TAXAMOV) berechnen
         * ---------------------------------------------------------------------- */
        BigDecimal totalTax = 0G
        resp.'**'.findAll { it.name() == 'TaxAmount' }.each {
            totalTax += new BigDecimal(it.text())
        }
        String taxamov = formatSigned(totalTax * multiplier)

        /* -------------------------------------------------------------------------
         *  RESULT_ITEM_JUR generieren
         * ---------------------------------------------------------------------- */
        def resultItems = []
        resp.'**'.findAll { it.name() == 'Tax' }.each { tax ->
            def lineNo       = tax.parent().parent().LineNumber.text()
            BigDecimal revenueSum = new BigDecimal(tax.Revenue.text())
            int taxCnt       = tax.NumberOfTaxes.text().toInteger()
            int groupCnt     = tax.NumberOfGroups.text().toInteger()
            int divisor      = groupCnt ? (taxCnt / groupCnt) : 1
            BigDecimal exemptAmount = revenueSum / divisor

            BigDecimal percentTaxable   = new BigDecimal(tax.PercentTaxable.text())
            BigDecimal examtCalc        = (exemptAmount -
                                           (exemptAmount * percentTaxable)) * multiplier
            BigDecimal taxAmtCalc       = new BigDecimal(tax.TaxAmount.text()) * multiplier

            resultItems << [
                    itemNo : lineNo,
                    examt  : formatSigned(examtCalc),
                    taxamt : formatSigned(taxAmtCalc),
                    excode : tax.ExemptCode.text()
            ]
        }

        /* -------------------------------------------------------------------------
         *  Ziel-XML aufbauen
         * ---------------------------------------------------------------------- */
        def sw = new StringWriter()
        def mb = new MarkupBuilder(sw)
        mb.'TAX_CALCULATION_RECEIVE'('xmlns': 'http://sap.com/xi/FotETaxUS') {
            'CALCULATION_RESULT_ITEM' {
                'TAXAMOV'(taxamov)
            }
            resultItems.each { itm ->
                'RESULT_ITEM_JUR' {
                    'ITEM_NO'(itm.itemNo)
                    'EXAMT'(itm.examt)
                    'TAXAMT'(itm.taxamt)
                    'EXCODE'(itm.excode)
                }
            }
        }
        return XmlUtil.serialize(sw.toString())
    } catch (Exception e) {
        handleError(ctx.messageLog, e, 'Fehler im Response-Mapping')
    }
}

/** Formatiert BigDecimal und verschiebt ggf. das Minus ans Ende */
def String formatSigned(BigDecimal value) {
    if (value == null) return ''
    String strVal = value.setScale(0, BigDecimal.ROUND_HALF_EVEN).toPlainString().trim()
    return strVal.startsWith('-') ? "${strVal.substring(1)}-" : strVal
}

// =======================================================================================
//  Modul 5: Logging
// =======================================================================================
/**
 *  Hängt eine beliebige Nachricht als String-Attachment an das MessageLog an.
 */
def void addLogAttachment(Message message, String name, String content) {
    def msgLog = messageLogFactory.getMessageLog(message)
    msgLog?.addAttachmentAsString(name, content, 'text/xml')
}

// =======================================================================================
//  Modul 6: Zentrales Error-Handling
// =======================================================================================
/**
 *  Fügt den ursprünglichen Payload als Attachment "ErrorPayload" hinzu und wirft
 *  eine RuntimeException mit aussagekräftiger Meldung.
 */
def handleError(def messageLog, Exception e, String msgPrefix) {
    messageLog?.addAttachmentAsString('ErrorPayload', e?.getMessage(), 'text/plain')
    throw new RuntimeException("${msgPrefix}: ${e.message}", e)
}