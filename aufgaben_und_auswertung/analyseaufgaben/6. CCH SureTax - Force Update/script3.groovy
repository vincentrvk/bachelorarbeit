/****************************************************************************************
 *  Groovy-Skript – S/4HANA CPI → CCH Sure Tax  (Force Update)
 *  Autor:  Senior-Developer Integration & Groovy
 *
 *  Beschreibung:
 *  Modul-basierte Umsetzung der Anforderungen gem. Aufgabenstellung.
 *  Jeder Schritt ist gekapselt, umfangreich kommentiert und mit
 *  aussagekräftigen Fehlermeldungen versehen.
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern
import javax.xml.transform.stream.StreamSource

Message processData(Message message) {

    /*--------------------------------------------------------------
      Initialisierung allgemeiner Objekte
    --------------------------------------------------------------*/
    final def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /*----------------------------------------------------------
          1. Schritt – Eingehenden Payload sichern & loggen
        ----------------------------------------------------------*/
        final String incomingBody = message.getBody(String)
        logAttachment(messageLog, "01_Incoming_Payload.xml", incomingBody)

        /*----------------------------------------------------------
          2. Schritt – Properties / Header lesen & validieren
        ----------------------------------------------------------*/
        final Map context = readAndValidateContext(message)

        /*----------------------------------------------------------
          3. Schritt – REQUEST-MAPPING erstellen
        ----------------------------------------------------------*/
        final String requestXml = createRequestMapping(incomingBody, context)
        logAttachment(messageLog, "02_Request_Mapping.xml", requestXml)

        /*----------------------------------------------------------
          4. Schritt – Aufruf CCH Sure Tax ForceUpdate
        ----------------------------------------------------------*/
        final String rawResponse = callSureTaxApi(requestXml, context)
        logAttachment(messageLog, "03_Raw_Response.xml", rawResponse)

        /*----------------------------------------------------------
          5. Schritt – RESPONSE-MAPPING erstellen
        ----------------------------------------------------------*/
        final String responseXml = createResponseMapping(rawResponse, context)
        logAttachment(messageLog, "04_Response_Mapping.xml", responseXml)

        /*----------------------------------------------------------
          6. Schritt – Response in die Nachricht schreiben
        ----------------------------------------------------------*/
        message.setBody(responseXml)
        return message

    } catch (Exception ex) {
        /*----------------------------------------------------------
          Zentrales Error-Handling
        ----------------------------------------------------------*/
        handleError(message.getBody(String), ex, messageLog)
    }
}

/*-----------------------------------------------------------------------------
 *  Funktionsblock: Kontext (Header / Properties) ermitteln & validieren
 *---------------------------------------------------------------------------*/
private Map readAndValidateContext(Message msg) {
    Map<String, Object> ctx = [:]

    // Helfer Closure – liest Header / Property oder liefert "placeholder"
    def readValue = { String key, boolean fromProp ->
        def val = fromProp ? msg.getProperty(key) : msg.getHeader(key, Object)
        (val == null || val.toString().trim().isEmpty()) ? "placeholder" : val.toString()
    }

    ctx.sureTaxUsername          = readValue('sureTaxUsername', true)
    ctx.sureTaxPassword          = readValue('sureTaxPassword', true)
    ctx.sureTaxURL               = readValue('sureTaxURL', true)
    ctx.exchangeCurrencyDecimal  = readValue('exchageCurrencyDecimal', true)
    ctx.exchangeJcdUnifyInd      = readValue('exchangeJcdUnifyInd', true)
    ctx.exchangeTID              = readValue('exchangeTID', true)

    // Pflicht-Properties prüfen
    if (ctx.exchangeJcdUnifyInd == 'placeholder') {
        throw new IllegalStateException('Pflicht-Property exchangeJcdUnifyInd fehlt!')
    }
    if (ctx.exchangeTID == 'placeholder') {
        throw new IllegalStateException('Pflicht-Property exchangeTID fehlt!')
    }
    return ctx
}

/*-----------------------------------------------------------------------------
 *  Funktionsblock: Request-Mapping
 *---------------------------------------------------------------------------*/
private String createRequestMapping(String sourceXml, Map ctx) {

    // Namespaces nicht strikt notwendig -> Slurper ohne Namespacehandling
    def slurper = new XmlSlurper(false, false)
    def srcRoot = slurper.parseText(sourceXml)

    /*------------------------- DataYear bestimmen ---------------------------*/
    def firstTaxDate = srcRoot.FORCE_ITEM[0].TAX_DATE.text()
    int dataYear
    if (firstTaxDate) {
        LocalDate date = LocalDate.parse(firstTaxDate)
        dataYear = (date.isAfter(LocalDate.now())) ? LocalDate.now().getYear()
                                                   : date.getYear()
    } else {
        dataYear = LocalDate.now().getYear()
    }

    /*------------------------- Skalierungsfaktor ----------------------------*/
    int currDec = ctx.exchangeCurrencyDecimal?.isNumber() ? ctx.exchangeCurrencyDecimal.toInteger() : 0
    BigDecimal scaleFactor = BigDecimal.valueOf(Math.pow(10, currDec))

    /*------------------------- Builder erstellen ---------------------------*/
    StringWriter sw = new StringWriter()
    MarkupBuilder mb = new MarkupBuilder(sw)
    mb.'ns1:SoapTaxAdjustmentWithReturnFileCodeRequest'('xmlns:ns1': 'http://example.namespace/ns1') {
        'ns1:request' {
            'ns1:DataYear'(dataYear)

            'ns1:TaxAdjustmentItemList' {

                srcRoot.FORCE_ITEM.each { item ->
                    String creditInd = item.CREDIT_IND.text()
                    /*--- Hilfsfunktionen für Zahlenwerte innerhalb der Schleife ---*/
                    Closure<BigDecimal> bd = { String s ->
                        (s == null || s.trim().isEmpty()) ? BigDecimal.ZERO :
                                new BigDecimal(s.trim().replace(',', '.'))
                    }
                    BigDecimal amount     = bd(item.AMOUNT.text())
                    BigDecimal freight    = bd(item.FREIGHT_AM.text())
                    BigDecimal exempt     = bd(item.EXEMPT_AMT.text())
                    BigDecimal taxamov    = bd(item.TAXAMOV.text())

                    // Summen & Skalierung
                    BigDecimal revenue     = (amount + freight - exempt) / scaleFactor
                    BigDecimal taxValue    = (taxamov) / scaleFactor

                    if (creditInd != '0' && !creditInd.isEmpty()) {
                        revenue  = revenue.negate()
                        taxValue = taxValue.negate()
                    }

                    /*---------------- Geocode Validierung -----------------------*/
                    String geoCode = transformGeocode(item.TXJCD_POO.text(), ctx.exchangeJcdUnifyInd)

                    /*---------------- XML-Erzeugung pro Item -------------------*/
                    'ns1:TaxAdjustmentItem' {
                        'ns1:Revenue'(revenue.stripTrailingZeros().toPlainString())
                        'ns1:Tax'(taxValue.stripTrailingZeros().toPlainString())
                        'ns1:OrderPlacementAddress' {
                            'ns1:Geocode'(geoCode)
                        }
                    }
                }
            }
        }
    }
    return sw.toString()
}

/*-----------------------------------------------------------------------------
 *  Funktionsblock: Geocode Transformation & Validierung
 *---------------------------------------------------------------------------*/
private String transformGeocode(String source, String unifyInd) {

    if (source == null || source.trim().isEmpty()) {
        throw new IllegalArgumentException('TXJCD_POO ist leer – Geocode erforderlich!')
    }
    String geo = source.trim()

    if ('X'.equalsIgnoreCase(unifyInd) && geo.length() >= 4) {
        // Zeichen 3 & 4 entfernen
        geo = geo[0..1] + geo[4..-1]
    }

    if (geo.length() != 12) {
        throw new IllegalArgumentException("Geocode Länge != 12 Zeichen: ${geo}")
    }

    Pattern pattern = Pattern.compile('^[A-Za-z]{2}\\d{5}[A-Za-z0-9]{5}$')
    if (!pattern.matcher(geo).matches()) {
        throw new IllegalArgumentException("Geocode Format ungültig: ${geo}")
    }
    return geo
}

/*-----------------------------------------------------------------------------
 *  Funktionsblock: Aufruf Sure Tax API (HTTP POST)
 *---------------------------------------------------------------------------*/
private String callSureTaxApi(String body, Map ctx) {

    URL url = new URL(ctx.sureTaxURL)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)

    // Basic-Auth Header
    String auth = "${ctx.sureTaxUsername}:${ctx.sureTaxPassword}"
    String enc  = auth.bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${enc}")
    conn.setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')

    // Body schreiben
    conn.outputStream.withWriter('UTF-8') { it << body }

    int rc = conn.responseCode
    if (rc != 200) {
        String errResp = conn.errorStream?.getText('UTF-8')
        throw new RuntimeException("HTTP-Fehler ${rc} bei Aufruf Sure Tax: ${errResp}")
    }
    return conn.inputStream.getText('UTF-8')
}

/*-----------------------------------------------------------------------------
 *  Funktionsblock: Response-Mapping
 *---------------------------------------------------------------------------*/
private String createResponseMapping(String rawResponse, Map ctx) {

    /* Hinweis:
       Die Aufgabe fordert kein spezifisches Parsen des Sure-Tax-Responses.
       Das Mapping erstellt lediglich den erwarteten Ziel-Header.          */
    StringWriter sw = new StringWriter()
    MarkupBuilder mb = new MarkupBuilder(sw)

    mb.'ns2:TAX_FORCE_RECEIVE'('xmlns:ns2': 'http://sap.com/xi/FotETaxUS') {
        'ns2:FORCE_RESULT_HEADER' {
            'ns2:API_VERSION'('1.0')
            'ns2:TID'(ctx.exchangeTID)
            'ns2:RETCODE'('0')
            'ns2:ERRCODE'('0000')
        }
    }
    return sw.toString()
}

/*-----------------------------------------------------------------------------
 *  Funktionsblock: Logging – Anhängen an Message-Monitoring
 *---------------------------------------------------------------------------*/
private void logAttachment(def msgLog, String name, String content) {
    msgLog?.addAttachmentAsString(name, content, 'text/plain')
}

/*-----------------------------------------------------------------------------
 *  Funktionsblock: Zentrales Error-Handling (gem. Vorgabe)
 *---------------------------------------------------------------------------*/
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    String errorMsg = "Fehler im Integration-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}