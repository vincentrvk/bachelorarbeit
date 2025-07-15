/****************************************************************************
 *  Groovy-Skript: Quote-Import SAP CPQ  ->  SAP Commerce Cloud
 *  Autor:        CPI – Senior Integration Developer
 *  Beschreibung: Siehe Aufgabenstellung
 ****************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets
import java.util.Base64
import groovy.xml.MarkupBuilder
import groovy.xml.XmlUtil
import groovy.xml.StreamingMarkupBuilder

// ============================   HAUPTFUNKTION   ============================
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1) Header & Property Handling */
        setContextValues(message, messageLog)

        /* 2) Optional Base64-Decoding */
        decodeQuoteCover(message, messageLog)

        /* Ursprüngliches Quote-XML lesen */
        def quoteSrcXml = message.getBody(String) ?: ''
        def quoteSrc    = new XmlSlurper().parseText(quoteSrcXml)

        /* 3) Mapping – Quote */
        logPayload(messageLog, 'Before_Quote_Mapping', quoteSrcXml)
        def quoteRequestXml = buildQuoteXml(message, quoteSrc)
        logPayload(messageLog, 'After_Quote_Mapping', quoteRequestXml)

        /* 4) API-Call – Quote */
        sendQuote(message, quoteRequestXml, messageLog)

        /* 5) Mapping & API-Call – Quote Entries */
        quoteSrc.Items.Item.eachWithIndex { item, idx ->
            logPayload(messageLog, "Before_QuoteEntry_Mapping_${idx}", XmlUtil.serialize(item))
            def entryRequestXml = buildQuoteEntryXml(message, quoteSrc, item)
            logPayload(messageLog, "After_QuoteEntry_Mapping_${idx}", entryRequestXml)

            sendQuoteEntry(message, entryRequestXml, messageLog, idx)
        }

    } catch (Exception e) {
        handleError(message.getBody(String), e, messageLog)
    }
    return message
}

// ============================   CONTEXT   ==================================
/*  Legt Properties & Header-Werte fest                                        */
void setContextValues(Message message, def messageLog) {
    ['requestUser', 'requestPassword', 'requestURL'].each { key ->
        def value = message.getProperty(key) ?: message.getHeader(key, String.class) ?: 'placeholder'
        message.setProperty(key, value)
    }
    logPayload(messageLog, 'Context_Properties', "URL=${message.getProperty('requestURL')}")
}

// ============================   BASE64-DECODE   =============================
/*  Dekodiert die Quote-Cover Datei und legt sie als Attachment ab            */
void decodeQuoteCover(Message message, def messageLog) {
    def xml = message.getBody(String)
    def quote = new XmlSlurper().parseText(xml)
    def encoded = quote.SubmitQuoteCover?.Content?.text()?.trim()
    if (encoded) {
        message.setProperty('encodedDocument', encoded)
        byte[] decodedBytes = Base64.decoder.decode(encoded)
        def decodedStr = new String(decodedBytes, StandardCharsets.UTF_8)
        messageLog?.addAttachmentAsString('Decoded_Quote_Cover', decodedStr, 'text/plain')
    }
}

// ============================   MAPPINGS   ==================================
/*  MAPPING – Quote                                                           */
String buildQuoteXml(Message message, def quoteSrc) {

    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)

    xml.Quotes('xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance') {
        Quote {
            /* externalQuoteId */
            def composite = quoteSrc.CompositeNumber.text().trim()
            def extId     = quoteSrc.QuoteExternalId.text().trim()
            externalQuoteId((composite == extId) ? '' : composite)

            /* externalQuoteDocument */
            externalQuoteDocument(message.getProperty('encodedDocument') ?: '')

            /* code */
            code(extId ? extId : composite)

            /* user / uid */
            user {
                User {
                    uid(quoteSrc.BillToCustomer.Email.text().trim())
                }
            }

            /* salesOrganization */
            salesOrganization(quoteSrc.SalesArea.SalesOrganization.text().trim())
        }
    }
    return writer.toString()
}

/*  MAPPING – Quote Entry                                                     */
String buildQuoteEntryXml(Message message, def quoteSrc, def item) {

    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)

    xml.QuoteEntries {
        QuoteEntry {
            /* Order / Quote / Code */
            order {
                Quote {
                    def composite = quoteSrc.CompositeNumber.text().trim()
                    def extId     = quoteSrc.QuoteExternalId.text().trim()
                    code(extId ? extId : composite)
                }
            }

            /* Rank */
            def rankVal = Math.ceil((item.Rank.text() as BigDecimal)).intValue() - 1
            rank(rankVal)

            /* entryDiscount & basePrice */
            entryDiscount(item.RolledUpDiscountAmount.DecimalValue.text().trim())
            basePrice(item.ListPrice.DecimalValue.text().trim())

            /* Quantity  (nach Beispiel ohne -1) */
            def qtyVal = Math.ceil((item.Quantity.text() as BigDecimal)).intValue()
            quantity(qtyVal)
        }
    }
    return writer.toString()
}

// ============================   API-CALLS   =================================
/*  Versendet den Quote-Request                                               */
void sendQuote(Message message, String payload, def messageLog) {
    logPayload(messageLog, 'Before_Quote_Request', payload)

    def url      = message.getProperty('requestURL')
    def user     = message.getProperty('requestUser')
    def password = message.getProperty('requestPassword')
    def conn     = new URL(url).openConnection()
    conn.setRequestMethod('POST')
    conn.doOutput = true
    conn.setRequestProperty('Authorization', 'Basic ' +
            "${user}:${password}".bytes.encodeBase64().toString())
    conn.outputStream.withWriter('UTF-8') { it << payload }
    def responseCode = conn.responseCode
    def responseBody = conn.inputStream?.getText('UTF-8') ?: ''
    logPayload(messageLog, 'After_Quote_Response', "RC=${responseCode}\n${responseBody}")
}

/*  Versendet einen Quote-Entry-Request                                       */
void sendQuoteEntry(Message message, String payload, def messageLog, int idx) {
    logPayload(messageLog, "Before_QuoteEntry_Request_${idx}", payload)

    def url      = message.getProperty('requestURL')
    def user     = message.getProperty('requestUser')
    def password = message.getProperty('requestPassword')
    def conn     = new URL(url).openConnection()
    conn.setRequestMethod('POST')
    conn.doOutput = true
    conn.setRequestProperty('Authorization', 'Basic ' +
            "${user}:${password}".bytes.encodeBase64().toString())
    conn.outputStream.withWriter('UTF-8') { it << payload }
    def responseCode = conn.responseCode
    def responseBody = conn.inputStream?.getText('UTF-8') ?: ''
    logPayload(messageLog, "After_QuoteEntry_Response_${idx}", "RC=${responseCode}\n${responseBody}")
}

// ============================   LOGGING   ===================================
/*  Fügt Payloads als Attachment hinzu                                        */
void logPayload(def messageLog, String name, String payload) {
    messageLog?.addAttachmentAsString(name, payload, 'text/xml')
}

// ============================   ERROR HANDLING ==============================
/*  Gibt Payload als Attachment weiter und wirft Exception                    */
void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Quote-Import-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}