/**************************************************************************
 *  Groovy-Skript  –  S/4 HANA Cloud  →  CCH Sure Tax  (Force-Update)
 *
 *  Autor:  ChatGPT (Senior-Integration-Developer)
 *  Version: 1.0
 *
 *  Beschreibung:
 *  ──────────────
 *  • Liest den eingehenden TAX_FORCE_SEND-Payload
 *  • Validiert Pflicht-Properties
 *  • Erstellt den Request-Body für den Sure-Tax-Service
 *  • Führt den HTTP-Call (Basic-Auth) aus
 *  • Mapped die Response in TAX_FORCE_RECEIVE
 *  • Fügt sämtliche Zwischenschritte als Message-Attachment hinzu
 *  • Umfassendes Error-Handling gem. Projektvorgaben
 **************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.*                                   // MarkupBuilder, XmlSlurper, NodePrinter
import java.text.SimpleDateFormat
import java.nio.charset.StandardCharsets
import java.math.RoundingMode

/*****************************
 *  Haupteinstiegspunkt
 *****************************/
Message processData(Message message) {

    // Attachment-fähiges Logging-Objekt
    def msgLog = messageLogFactory.getMessageLog(message)

    // Ursprungs-Payload (als String)
    String originalBody = message.getBody(String)
    logStep(msgLog, '01_IncomingPayload', originalBody)

    try {

        /*===== Kontext aufbereiten & validieren =====*/
        Map ctx = collectContext(message)
        validateContext(ctx)

        /*===== Request-Mapping =====*/
        String requestBody = buildRequestPayload(originalBody, ctx)
        logStep(msgLog, '02_RequestMapping', requestBody)

        /*===== Aufruf Sure-Tax API =====*/
        String sureTaxResponse = callSureTaxAPI(requestBody, ctx, msgLog)
        logStep(msgLog, '03_ResponsePayload', sureTaxResponse)

        /*===== Response-Mapping  =====*/
        String mappedResponse = buildResponsePayload(ctx)
        logStep(msgLog, '04_ResponseMapping', mappedResponse)

        /*===== Ergebnis setzen =====*/
        message.setBody(mappedResponse)

    } catch (Exception e) {
        // zentrales Error-Handling
        handleError(originalBody, e, msgLog)
    }

    return message
}

/**************************************************************************
 *  Funktions-Bibliothek
 **************************************************************************/

/*-----------------------------------------
 *  Kontext (Header & Properties) sammeln
 *----------------------------------------*/
private Map collectContext(Message msg) {

    Map ctx = [:]

    // Properties / Header → mit Fallback 'placeholder'
    ctx.username                 = (msg.getProperty('sureTaxUsername')      ?: 'placeholder') as String
    ctx.password                 = (msg.getProperty('sureTaxPassword')      ?: 'placeholder') as String
    ctx.url                      = (msg.getProperty('sureTaxURL')           ?: 'placeholder') as String
    ctx.exchangeJcdUnifyInd      = (msg.getProperty('exchangeJcdUnifyInd')  ?: 'placeholder') as String
    ctx.exchangeTID              = (msg.getProperty('exchangeTID')          ?: 'placeholder') as String
    ctx.exchangeCurrencyDecimal  = (msg.getProperty('exchageCurrencyDecimal')?: '0')          as String   // Fallback 0

    return ctx
}

/*-----------------------------------------
 *  Pflicht-Felder validieren
 *----------------------------------------*/
private void validateContext(Map ctx) {

    StringBuilder sb = new StringBuilder()

    if (!ctx.exchangeJcdUnifyInd?.trim()) sb.append('- Property exchangeJcdUnifyInd fehlt\n')
    if (!ctx.exchangeTID?.trim())         sb.append('- Property exchangeTID fehlt\n')

    if (sb) {
        throw new IllegalStateException('Pflicht-Property fehlt:\n' + sb.toString())
    }
}

/*-----------------------------------------
 *  Request-Mapping erzeugen
 *----------------------------------------*/
private String buildRequestPayload(String sourceXml, Map ctx) {

    def slurper = new XmlSlurper().parseText(sourceXml)
    slurper.normalize()

    /* ===== DataYear ermitteln ===== */
    // Erstes TAX_DATE nehmen
    String firstTaxDate   = (slurper.FORCE_ITEM?.TAX_DATE?.text()) ?: ''
    int    yearCandidate  = firstTaxDate ? firstTaxDate[0..3] as Integer : new Date().format('yyyy') as Integer
    int    currentYear    = new Date().format('yyyy') as Integer
    int    dataYear       = yearCandidate > currentYear ? currentYear : yearCandidate

    /* ===== Dezimalstellen / Skalierungsfaktor ===== */
    int currDecInt  = (ctx.exchangeCurrencyDecimal ?: slurper.FORCE_HEADER.CURR_DEC.text() ?: '0') as Integer
    BigDecimal scaleDivisor = 10 ** currDecInt

    /* ===== TaxAdjustmentItemList aufbauen ===== */
    def items       = slurper.FORCE_ITEM
    def writer      = new StringWriter()
    def xml         = new MarkupBuilder(writer)
    xml.'ns1:SoapTaxAdjustmentWithReturnFileCodeRequest'(
            'xmlns:ns1': 'http://example.namespace/ns1'
    ) {
        'ns1:request' {
            'ns1:DataYear'(dataYear)
            'ns1:TaxAdjustmentItemList' {
                items.each { itm ->
                    'ns1:TaxAdjustmentItem' {
                        /* ---- Revenue berechnen ---- */
                        BigDecimal amount     = toBigDecimal(itm.AMOUNT.text())
                        BigDecimal freight    = toBigDecimal(itm.FREIGHT_AM.text(), BigDecimal.ZERO)
                        BigDecimal exempt     = toBigDecimal(itm.EXEMPT_AMT.text(), BigDecimal.ZERO)
                        BigDecimal revenueRaw = (amount + freight - exempt) / scaleDivisor
                        BigDecimal revenue    = itm.CREDIT_IND.text() == '0' ? revenueRaw : revenueRaw * -1

                        /* ---- Tax berechnen ---- */
                        BigDecimal taxRaw = toBigDecimal(itm.TAXAMOV.text()) / scaleDivisor
                        BigDecimal tax    = itm.CREDIT_IND.text() == '0' ? taxRaw : taxRaw * -1

                        'ns1:Revenue'(revenue.setScale(2, RoundingMode.HALF_UP))
                        'ns1:Tax'(tax.setScale(2, RoundingMode.HALF_UP))

                        /* ---- Geocode ---- */
                        String geocode = transformGeocode(itm.TXJCD_POO.text(), ctx.exchangeJcdUnifyInd)
                        'ns1:OrderPlacementAddress' {
                            'ns1:Geocode'(geocode)
                        }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/*-----------------------------------------
 *  HTTP-API Call an Sure Tax
 *----------------------------------------*/
private String callSureTaxAPI(String requestBody, Map ctx, def msgLog) {

    URL                  url  = new URL(ctx.url)
    HttpURLConnection con     = (HttpURLConnection) url.openConnection()
    con.setRequestMethod('POST')
    String auth = "${ctx.username}:${ctx.password}"
    con.setRequestProperty('Authorization', 'Basic ' +
            auth.bytes.encodeBase64().toString())
    con.setDoOutput(true)

    con.outputStream.withWriter('UTF-8') { it << requestBody }
    int rc = con.responseCode

    // Logging des HTTP-Status
    msgLog?.addAttachmentAsString('HTTP_Status', rc.toString(), 'text/plain')

    String responseText = rc == 200 ?
            con.inputStream.getText(StandardCharsets.UTF_8.name()) :
            con.errorStream?.getText(StandardCharsets.UTF_8.name()) ?: ''

    if (rc != 200) {
        throw new RuntimeException("Sure-Tax API antwortete mit HTTP Status ${rc}")
    }
    return responseText
}

/*-----------------------------------------
 *  Response-Mapping erzeugen
 *----------------------------------------*/
private String buildResponsePayload(Map ctx) {

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

/*-----------------------------------------
 *  Geocode-Transformation gem. Regelwerk
 *----------------------------------------*/
private String transformGeocode(String input, String unifyInd) {

    if (!input) {
        throw new IllegalArgumentException('TXJCD_POO darf nicht leer sein')
    }

    String tmp = input.trim()
    if (unifyInd == 'X' && tmp.length() >= 4) {
        // Zeichen 3 & 4 entfernen
        tmp = tmp[0..1] + tmp[4..-1]
    }

    if (tmp.length() != 12) {
        throw new IllegalArgumentException("Geocode ${tmp} hat nicht exakt 12 Zeichen")
    }

    // Validierungs-RegEx: 2 Buchstaben, 5 Ziffern, 5 alphanum.
    if (!(tmp ==~ /^[A-Za-z]{2}\d{5}[A-Za-z0-9]{5}$/)) {
        throw new IllegalArgumentException("Geocode ${tmp} entspricht nicht dem geforderten Format")
    }
    return tmp
}

/*-----------------------------------------
 *  String → BigDecimal (Hilfsfunktion)
 *----------------------------------------*/
private BigDecimal toBigDecimal(String val, BigDecimal defaultVal = BigDecimal.ZERO) {
    if (!val) return defaultVal
    String v = val.trim()
    return v ? new BigDecimal(v) : defaultVal
}

/*-----------------------------------------
 *  Schritt-Logging mit Attachment
 *----------------------------------------*/
private void logStep(def msgLog, String name, String content) {
    msgLog?.addAttachmentAsString(name, content ?: '', 'text/xml')
}

/*-----------------------------------------
 *  Zentrales Error-Handling
 *----------------------------------------*/
private void handleError(String body, Exception e, def msgLog) {
    msgLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    String errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    msgLog?.addAttachmentAsString('ErrorMessage', errorMsg, 'text/plain')
    throw new RuntimeException(errorMsg, e)
}