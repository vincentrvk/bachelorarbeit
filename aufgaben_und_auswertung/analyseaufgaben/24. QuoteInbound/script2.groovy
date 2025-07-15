/*************************************************
 *  Groovy-Script für SAP Cloud Integration (CPI)
 *  Quote-Import von SAP CPQ nach SAP Commerce Cloud
 *  Autor: Senior Integration Developer
 *************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import groovy.xml.XmlUtil

/*******************************
 *  Haupteinstiegspunkt
 *******************************/
Message processData(Message message) {

    /* Message-Log instanziieren */
    def messageLog = messageLogFactory?.getMessageLog(message)

    try {

        /* Ursprüngliches Payload einlesen */
        String inBody = message.getBody(String) ?: ''
        def quoteXml  = new XmlSlurper().parseText(inBody)

        /* Header & Properties absichern                                                */
        setPropertiesAndHeaders(message, messageLog)

        /* Dokument ggf. dekodieren & Property setzen                                  */
        String encodedDocument = extractAndDecodeDocument(message, quoteXml, messageLog)

        /***************
         *  QUOTE
         ***************/
        addLog(messageLog, 'BeforeMappingQuote', inBody)
        String quoteRequestBody = buildQuoteXml(quoteXml, encodedDocument)
        addLog(messageLog, 'AfterMappingQuote', quoteRequestBody)

        /* Quote an Commerce senden                                                    */
        sendQuote(message, quoteRequestBody, messageLog)

        /***************
         *  QUOTE-ENTRIES
         ***************/
        quoteXml?.Items?.Item?.eachWithIndex { item, idx ->

            addLog(messageLog, "BeforeMappingQuoteEntry_${idx}", XmlUtil.serialize(item))
            String entryPayload = buildQuoteEntryXml(quoteXml, item)
            addLog(messageLog, "AfterMappingQuoteEntry_${idx}", entryPayload)

            /* Einzelnen Quote-Entry senden                                            */
            sendQuoteEntry(message, entryPayload, messageLog, idx)
        }

        return message

    } catch (Exception e) {
        /* Fehlerbehandlung                                                            */
        handleError(message.getBody(String) as String, e, messageLog)
    }
}

/********************************************************************
 *  Funktion: Header & Properties setzen
 ********************************************************************/
void setPropertiesAndHeaders(Message msg, def log) {

    def ensure = { propName ->
        def val = msg.getProperty(propName) ?: 'placeholder'
        msg.setProperty(propName, val)
    }

    ['requestUser', 'requestPassword', 'requestURL'].each { ensure(it) }

    addLog(log, 'PropertiesAfterInit',
           "requestUser=${msg.getProperty('requestUser')}, requestURL=${msg.getProperty('requestURL')}")
}

/********************************************************************
 *  Funktion: Base64-Dokument extrahieren, dekodieren & loggen
 ********************************************************************/
String extractAndDecodeDocument(Message msg, def xml, def log) {

    def contentNode = xml?.SubmitQuoteCover?.Content
    if (!contentNode || !contentNode.text()) {
        msg.setProperty('encodedDocument', '')
        return ''
    }

    String encoded = contentNode.text().trim()
    msg.setProperty('encodedDocument', encoded)

    /* Dekodieren (wenn möglich) und als Attachment protokollieren                     */
    try {
        String decoded = new String(encoded.decodeBase64(), 'UTF-8')
        addLog(log, 'DecodedQuoteCover', decoded)
    } catch (Exception ignore) {
        addLog(log, 'DecodedQuoteCover', 'Dekodierung nicht möglich oder fehlerhaft.')
    }

    return encoded
}

/********************************************************************
 *  Funktion: Quote-XML aufbauen
 ********************************************************************/
String buildQuoteXml(def xml, String encodedDocument) {

    StringWriter sw = new StringWriter()
    MarkupBuilder mb = new MarkupBuilder(sw)
    mb.doubleQuotes = true

    mb.Quotes(xmlns:xsi:'http://www.w3.org/2001/XMLSchema-instance') {
        Quote {
            externalQuoteId(determineExternalQuoteId(xml))
            externalQuoteDocument(encodedDocument ?: '')
            code(determineQuoteCode(xml))
            user {
                User {
                    uid(xml?.BillToCustomer?.Email?.text()?.trim() ?: '')
                }
            }
            salesOrganization(xml?.SalesArea?.SalesOrganization?.text()?.trim() ?: '')
        }
    }
    return XmlUtil.serialize(sw.toString())
}

/********************************************************************
 *  Funktion: Quote-Entry-XML aufbauen
 ********************************************************************/
String buildQuoteEntryXml(def rootXml, def item) {

    StringWriter sw = new StringWriter()
    MarkupBuilder mb = new MarkupBuilder(sw)
    mb.doubleQuotes = true

    mb.QuoteEntries {
        QuoteEntry {
            order {
                Quote {
                    code(determineQuoteCode(rootXml))
                }
            }
            /* Rank: runden & 1 subtrahieren                                            */
            rank(Math.round(item?.Rank?.text()?.toBigDecimal() ?: 0) - 1)
            entryDiscount(item?.RolledUpDiscountAmount?.DecimalValue?.text()?.trim() ?: '')
            basePrice(item?.ListPrice?.DecimalValue?.text()?.trim() ?: '')

            /* Achtung: Fachliche Vorgabe widersprüchlich – hier ohne -1 gem. Beispiel  */
            quantity(Math.round(item?.Quantity?.text()?.toBigDecimal() ?: 0))
        }
    }
    return XmlUtil.serialize(sw.toString())
}

/********************************************************************
 *  Funktion: Quote versenden
 ********************************************************************/
void sendQuote(Message msg, String payload, def log) {

    addLog(log, 'BeforeRequest_Quote', payload)

    doHttpPost(msg, payload)

    addLog(log, 'AfterRequest_Quote', 'Quote gesendet.')
}

/********************************************************************
 *  Funktion: Quote-Entry versenden
 ********************************************************************/
void sendQuoteEntry(Message msg, String payload, def log, int idx) {

    addLog(log, "BeforeRequest_QuoteEntry_${idx}", payload)

    doHttpPost(msg, payload)

    addLog(log, "AfterRequest_QuoteEntry_${idx}", "QuoteEntry ${idx} gesendet.")
}

/********************************************************************
 *  Funktion: Tatsächlicher HTTP-POST-Aufruf
 ********************************************************************/
void doHttpPost(Message msg, String body) {

    String url       = msg.getProperty('requestURL')
    String user      = msg.getProperty('requestUser')
    String password  = msg.getProperty('requestPassword')
    String authValue = "${user}:${password}".bytes.encodeBase64().toString()

    HttpURLConnection conn = null
    try {
        conn = (HttpURLConnection) new URL(url).openConnection()
        conn.requestMethod = 'POST'
        conn.doOutput      = true
        conn.setRequestProperty('Authorization', "Basic ${authValue}")

        conn.outputStream.withWriter('UTF-8') { it << body }
        conn.connect()

        /* Keine Response-Verarbeitung laut Vorgabe                                    */
    } finally {
        conn?.inputStream?.close()
        conn?.disconnect()
    }
}

/********************************************************************
 *  Hilfsfunktion: ExternalQuoteId bestimmen
 ********************************************************************/
String determineExternalQuoteId(def xml) {

    String composite     = xml?.CompositeNumber?.text()?.trim() ?: ''
    String external      = xml?.QuoteExternalId?.text()?.trim() ?: ''

    return composite == external ? '' : composite
}

/********************************************************************
 *  Hilfsfunktion: Quote-Code bestimmen
 ********************************************************************/
String determineQuoteCode(def xml) {

    String composite = xml?.CompositeNumber?.text()?.trim() ?: ''
    String external  = xml?.QuoteExternalId?.text()?.trim() ?: ''

    return external ? external : composite
}

/********************************************************************
 *  Funktion: Logging als Message-Attachment
 ********************************************************************/
void addLog(def msgLog, String name, String content) {
    msgLog?.addAttachmentAsString(name, content ?: '', 'text/plain')
}

/********************************************************************
 *  Fehler-Behandlung gem. Vorgabe
 ********************************************************************/
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}