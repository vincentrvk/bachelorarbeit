/*************************************************************************************************
 * Groovy-Skript  –  SAP Cloud Integration (CPI)                                                  *
 * Aufgabe:  Import von Quotes & Quote Entries aus SAP CPQ in SAP Commerce Cloud                 *
 * Autor:    ChatGPT (Senior-Integration Developer)                                              *
 *************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import java.net.HttpURLConnection
import java.net.URL
import groovy.xml.MarkupBuilder

/* ==============================================================================================
 *  Haupt­einstieg
 * ============================================================================================ */
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* --------------------------------------------------------------------------
         * 1. Header & Property Handling
         * ------------------------------------------------------------------------ */
        setDefaults(message, messageLog)

        /* --------------------------------------------------------------------------
         * 2. Eingehenden Payload einlesen
         * ------------------------------------------------------------------------ */
        String rawPayload = message.getBody(String) ?: ''
        logAttachment(messageLog, 'InputPayload', rawPayload)

        def quoteXml = new XmlSlurper().parseText(rawPayload)

        /* --------------------------------------------------------------------------
         * 3. Dekodieren evtl. vorhandener Base64-Dokumente
         * ------------------------------------------------------------------------ */
        decodeDocument(quoteXml, message, messageLog)

        /* --------------------------------------------------------------------------
         * 4. Mapping – Quote
         * ------------------------------------------------------------------------ */
        logAttachment(messageLog, 'BeforeQuoteMapping', groovy.xml.XmlUtil.serialize(quoteXml))
        String quoteRequestXml = mapQuote(quoteXml, message)
        logAttachment(messageLog, 'AfterQuoteMapping', quoteRequestXml)

        /* --------------------------------------------------------------------------
         * 5. API-Aufruf – Quote
         * ------------------------------------------------------------------------ */
        callApi('SendQuote', quoteRequestXml, message, messageLog)

        /* --------------------------------------------------------------------------
         * 6. Mapping & API-Aufruf je Quote Entry
         * ------------------------------------------------------------------------ */
        int idx = 0
        quoteXml.Items.Item.each { item ->
            logAttachment(messageLog, "BeforeQuoteEntryMapping_${idx}", groovy.xml.XmlUtil.serialize(item))
            String entryRequestXml = mapQuoteEntry(quoteXml, item)
            logAttachment(messageLog, "AfterQuoteEntryMapping_${idx}", entryRequestXml)

            callApi("SendQuoteEntry_${idx}", entryRequestXml, message, messageLog)
            idx++
        }

    } catch (Exception e) {
        handleError(message.getBody(String), e, messageLog)
    }

    /* CPI setzt das Message-Objekt in den Flow zurück */
    return message
}

/* ==============================================================================================
 *  Hilfsfunktionen
 * ============================================================================================ */

/**
 * Legt Standardwerte für Properties & Header fest, sofern sie nicht vorhanden sind.
 */
void setDefaults(Message message, def messageLog) {
    ['requestUser', 'requestPassword', 'requestURL'].each { key ->
        if (!message.getProperty(key)) {
            message.setProperty(key, 'placeholder')
            logAttachment(messageLog, 'DefaultPropertySet', "Property '${key}' auf 'placeholder' gesetzt.")
        }
    }
}

/**
 * Dekodiert den Base64-Inhalt unter //Quote/SubmitQuoteCover/Content, legt ihn als Attachment
 * ab und speichert den kodierten String in der Property 'encodedDocument'.
 */
void decodeDocument(def quoteXml, Message message, def messageLog) {
    def contentNode = quoteXml?.SubmitQuoteCover?.Content
    if (contentNode && contentNode.text()?.trim()) {
        String encoded = contentNode.text().trim()
        message.setProperty('encodedDocument', encoded)

        try {
            String decoded = new String(encoded.decodeBase64(), 'UTF-8')
            logAttachment(messageLog, 'DecodedDocument', decoded)
        } catch (Exception e) {
            // Fehlgeschlagene Dekodierung wird geloggt, Prozess läuft weiter
            logAttachment(messageLog, 'DecodeError', "Dekodierung fehlgeschlagen: ${e.message}")
        }
    }
}

/**
 * Erstellt das Quote-Request-XML gem. Mapping-Spezifikation.
 */
String mapQuote(def qx, Message message) {

    String composite     = qx.CompositeNumber.text().trim()
    String externalId    = qx.QuoteExternalId.text().trim()
    String externalDoc   = (message.getProperty('encodedDocument') ?: 'placeholder')

    // externalQuoteId
    String externalQuoteId = composite.equalsIgnoreCase(externalId) ? '' : composite
    // code
    String codeValue = externalId ? externalId : composite

    StringWriter writer = new StringWriter()
    MarkupBuilder mb   = new MarkupBuilder(writer)
    mb.setDoubleQuotes(true)

    mb.Quotes('xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance') {
        Quote {
            externalQuoteId(externalQuoteId)
            externalQuoteDocument(externalDoc)
            code(codeValue)
            user {
                User {
                    uid(qx.BillToCustomer.Email.text().trim())
                }
            }
            salesOrganization(qx.SalesArea.SalesOrganization.text().trim())
        }
    }
    return writer.toString()
}

/**
 * Erstellt das QuoteEntry-Request-XML für ein einzelnes Item.
 */
String mapQuoteEntry(def rootXml, def item) {

    String composite   = rootXml.CompositeNumber.text().trim()
    String externalId  = rootXml.QuoteExternalId.text().trim()
    String quoteCode   = externalId ? externalId.trim() : composite

    int rankValue      = Math.ceil(item.Rank.text().toDouble()).intValue() - 1
    if (rankValue < 0) rankValue = 0   // Sicherstellen, dass Rank nicht negativ wird

    StringWriter writer = new StringWriter()
    MarkupBuilder mb   = new MarkupBuilder(writer)
    mb.setDoubleQuotes(true)

    mb.QuoteEntries {
        QuoteEntry {
            order {
                Quote {
                    code(quoteCode)
                }
            }
            rank(rankValue)
            entryDiscount(item.RolledUpDiscountAmount.DecimalValue.text().trim())
            basePrice(item.ListPrice.DecimalValue.text().trim())
            quantity(Math.ceil(item.Quantity.text().toDouble()).intValue())     // gem. Beispiel ohne -1
        }
    }
    return writer.toString()
}

/**
 * Führt den HTTP-POST Request aus und loggt Request & Response als Attachments.
 */
void callApi(String callName, String requestBody, Message message, def messageLog) {

    try {
        logAttachment(messageLog, "Request_${callName}", requestBody)

        String urlStr  = message.getProperty('requestURL')      ?: 'placeholder'
        String user    = message.getProperty('requestUser')     ?: 'placeholder'
        String pwd     = message.getProperty('requestPassword') ?: 'placeholder'

        URL url = new URL(urlStr)
        HttpURLConnection conn = (HttpURLConnection) url.openConnection()
        conn.setRequestMethod('POST')
        conn.setDoOutput(true)
        conn.setRequestProperty('Content-Type', 'application/xml')

        String auth = "${user}:${pwd}".bytes.encodeBase64().toString()
        conn.setRequestProperty('Authorization', "Basic ${auth}")

        conn.outputStream.withWriter('UTF-8') { it << requestBody }
        int httpCode = conn.responseCode

        // Response lesen (Input- oder Error-Stream abhängig vom Status)
        String responsePayload = ''
        try {
            responsePayload = conn.inputStream?.getText('UTF-8')
        } catch (Exception ignored) {
            responsePayload = conn.errorStream?.getText('UTF-8') ?: ''
        }

        logAttachment(messageLog, "Response_${callName}_Code", httpCode.toString())
        logAttachment(messageLog, "Response_${callName}_Body", responsePayload)

    } catch (Exception e) {
        handleError(requestBody, e, messageLog)
    }
}

/**
 * Fügt dem MessageLog einen Attachment-Eintrag hinzu (Null-Sicher).
 */
void logAttachment(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content ?: '', 'text/xml')
}

/**
 * Zentrales Error-Handling: Original-Payload anhängen und RuntimeException werfen.
 */
void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}