/**************************************************************************************************
 * Groovy-Skript SAP CI – Quote & QuoteEntry Integration Commerce → CPQ
 * Autor: AI Senior Developer
 **************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import java.nio.charset.StandardCharsets
import java.util.Base64
import groovy.xml.MarkupBuilder
import java.net.HttpURLConnection
import java.net.URL

// ================================================================================================
// Haupteinstieg
// ================================================================================================
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {

        // 1 – Header / Property Handling
        def (requestURL, requestUser, requestPassword) = setRequestParameters(message, messageLog)

        // 2 – Payload einlesen und parsen
        String incomingBody = message.getBody(String) ?: ''
        def xmlIn = new XmlSlurper().parseText(incomingBody)

        // 3 – Base64 Dokument dekodieren & speichern
        def encodedDocument = decodeDocument(xmlIn, message, messageLog)

        // 4 – Quote mappen und senden
        logPayload(messageLog, 'BeforeQuoteMapping', incomingBody)
        String quotePayload = mapQuote(xmlIn, encodedDocument)
        logPayload(messageLog, 'AfterQuoteMapping', quotePayload)
        sendRequest(requestURL, requestUser, requestPassword, quotePayload, messageLog, 'SendQuote')

        // 5 – Quote Entries mappen und senden
        xmlIn.Items.Item.eachWithIndex { item, idx ->
            String itemId = "Item${idx + 1}"
            logPayload(messageLog, "BeforeQuoteEntryMapping-${itemId}", groovy.xml.XmlUtil.serialize(item))
            String quoteEntryPayload = mapQuoteEntry(xmlIn, item)
            logPayload(messageLog, "AfterQuoteEntryMapping-${itemId}", quoteEntryPayload)
            sendRequest(requestURL, requestUser, requestPassword, quoteEntryPayload, messageLog, "SendQuoteEntry-${itemId}")
        }

        // Originalen Body erhalten
        message.setBody(incomingBody)

    } catch(Exception e) {
        handleError(message.getBody(String), e, messageLog)
    }

    return message
}

// ================================================================================================
// Modul 1 – Header / Property Handling
// ================================================================================================
/**
 * Setzt bzw. liest erforderliche Properties und gibt sie zurück.
 */
def setRequestParameters(Message message, def messageLog) {
    def requestURL      = (message.getProperty('requestURL')      ?: 'placeholder').toString()
    def requestUser     = (message.getProperty('requestUser')     ?: 'placeholder').toString()
    def requestPassword = (message.getProperty('requestPassword') ?: 'placeholder').toString()

    // als Header verfügbar machen
    ['requestURL':requestURL, 'requestUser':requestUser, 'requestPassword':requestPassword].each{ k,v -> message.setHeader(k,v) }

    return [requestURL, requestUser, requestPassword]
}

// ================================================================================================
// Modul 2 – Base64 Decoding
// ================================================================================================
/**
 * Dekodiert den Base64-Inhalt (sofern vorhanden) und speichert ihn als Attachment.
 */
def decodeDocument(def xml, Message message, def messageLog) {
    def encoded = xml.SubmitQuoteCover.Content?.text()
    if(encoded) {
        message.setProperty('encodedDocument', encoded)
        try {
            byte[] decodedBytes = encoded.decodeBase64()
            String decodedStr = new String(decodedBytes, StandardCharsets.UTF_8)
            logPayload(messageLog, 'DecodedDocument', decodedStr)
        } catch(Exception ex) {
            messageLog?.addAttachmentAsString('DecodeError', ex.message, 'text/plain')
        }
        return encoded        // für Mapping wird der originale Base64-String verwendet
    }
    return ''
}

// ================================================================================================
// Modul 3 – Mapping Quote
// ================================================================================================
/**
 * Erzeugt den Quote-Payload gemäß Zielschema.
 */
def mapQuote(def xml, String encodedDocument) {
    def compNr = xml.CompositeNumber.text().trim()
    def extId  = xml.QuoteExternalId.text().trim()

    def externalQuoteId = (compNr == extId) ? '' : compNr
    def code            = extId ? extId : compNr
    def email           = xml.BillToCustomer.Email.text().trim()
    def salesOrg        = xml.SalesArea.SalesOrganization.text().trim()

    def w = new StringWriter()
    def mb = new MarkupBuilder(w)
    mb.mkp.xmlDeclaration(version:'1.0', encoding:'UTF-8')
    mb.Quotes('xmlns:xsi':'http://www.w3.org/2001/XMLSchema-instance') {
        Quote {
            externalQuoteId(externalQuoteId)
            externalQuoteDocument(encodedDocument ?: '')
            code(code)
            user {
                User {
                    uid(email)
                }
            }
            salesOrganization(salesOrg)
        }
    }
    return w.toString()
}

// ================================================================================================
// Modul 4 – Mapping Quote Entry
// ================================================================================================
/**
 * Erzeugt den QuoteEntry-Payload für ein Item.
 */
def mapQuoteEntry(def xml, def item) {

    def compNr = xml.CompositeNumber.text().trim()
    def extId  = xml.QuoteExternalId.text().trim()
    def orderCode = extId ? extId : compNr

    int rank = 0
    def rankVal = item.Rank.text().toString().trim()
    if(rankVal) {
        rank = (int)Math.ceil(rankVal.toBigDecimal().doubleValue()) - 1
        if(rank < 0) rank = 0
    }

    def entryDiscount = item.RolledUpDiscountAmount.DecimalValue.text().trim()
    def basePrice     = item.ListPrice.DecimalValue.text().trim()
    def quantity      = item.Quantity.text().trim()

    def w = new StringWriter()
    def mb = new MarkupBuilder(w)
    mb.mkp.xmlDeclaration(version:'1.0', encoding:'UTF-8')
    mb.QuoteEntries {
        QuoteEntry {
            order {
                Quote {
                    code(orderCode)
                }
            }
            rank(rank.toString())
            entryDiscount(entryDiscount)
            basePrice(basePrice)
            quantity(quantity)
        }
    }
    return w.toString()
}

// ================================================================================================
// Modul 5 – HTTP API Call
// ================================================================================================
/**
 * Führt einen HTTP-POST mit Basic Authentication durch und loggt Request/Response.
 */
def sendRequest(String urlStr, String user, String pass, String payload, def messageLog, String tag) {

    logPayload(messageLog, "${tag}-RequestPayload", payload)

    HttpURLConnection conn
    try {
        conn = ( HttpURLConnection ) new URL(urlStr).openConnection()
        conn.with {
            requestMethod       = 'POST'
            doOutput            = true
            connectTimeout      = 30000
            readTimeout         = 60000
            setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
            String auth = Base64.encoder.encodeToString("${user}:${pass}".getBytes(StandardCharsets.UTF_8))
            setRequestProperty('Authorization', "Basic ${auth}")
        }

        conn.outputStream.withWriter('UTF-8') { it << payload }

        int rc = conn.responseCode
        String respBody = ''
        if(conn.inputStream) {
            respBody = conn.inputStream.getText('UTF-8')
        }

        logPayload(messageLog, "${tag}-ResponseCode", String.valueOf(rc))
        if(respBody) {
            logPayload(messageLog, "${tag}-ResponseBody", respBody)
        }

    } catch(Exception ex) {
        handleError(payload, ex, messageLog)
    } finally {
        conn?.disconnect()
    }
}

// ================================================================================================
// Modul 6 – Logging Helfer
// ================================================================================================
/**
 * Schreibt einen String-Payload als Attachment in das MessageLog.
 */
def logPayload(def messageLog, String name, String payload) {
    if(messageLog && payload != null) {
        messageLog.addAttachmentAsString(name, payload, 'text/xml')
    }
}

// ================================================================================================
// Modul 7 – Error Handling
// ================================================================================================
/**
 * Zentrales Error-Handling mit anhängendem Payload.
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}