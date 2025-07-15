/****************************************************************************************
 *  Skriptname  :  Quote_2_Commerce_Cloud.groovy
 *  Beschreibung:  Importiert Quotes & Quote-Entries aus SAP CPQ in SAP Commerce Cloud.
 *                 – Setzt benötigte Properties/Headers (User/Pass/URL)
 *                 – Dekodiert ggf. eingebettete Base64-Dokumente
 *                 – Erstellt Request-Payloads (Quote & QuoteEntry) via Mapping-Logik
 *                 – Sendet POST-Requests (Basic-Auth) an Commerce Cloud
 *                 – Umfassendes Logging & Fehlerbehandlung
 *
 *  Autor        :  Senior-Integrator (Groovy / SAP CPI)
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import java.net.HttpURLConnection
import java.net.URL
import groovy.xml.MarkupBuilder
import groovy.xml.XmlUtil

Message processData(Message message) {

    /*========== HILFSFUNKTIONEN =======================================================*/

    // 1) Fehlerbehandlung --------------------------------------------------------------
    def handleError = { String originalBody, Exception e, def messageLog ->
        messageLog?.addAttachmentAsString("ErrorPayload", originalBody ?: "", "text/xml")
        def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
        throw new RuntimeException(errorMsg, e)
    }

    // 2) Attachment-Logging ------------------------------------------------------------
    def logAttachment = { def messageLog, String name, String payload, String mime = 'text/plain' ->
        messageLog?.addAttachmentAsString(name, payload ?: "", mime)
    }

    // 3) Properties & Header auslesen / vorbelegen -------------------------------------
    def readContextData = { Message msg ->
        def getVal = { String key ->
            (msg.getProperty(key) ?: msg.getHeader(key, String) ?: 'placeholder') as String
        }
        return [
                url : getVal('requestURL'),
                user: getVal('requestUser'),
                pass: getVal('requestPassword')
        ]
    }

    // 4) Base64-Dekodierung ------------------------------------------------------------
    def decodeDocumentIfPresent = { def quoteXml, Message msg, def messageLog ->
        def encoded = quoteXml?.SubmitQuoteCover?.Content?.text()?.trim()
        if (encoded) {
            // Property für spätere Verwendung setzen
            msg.setProperty('encodedDocument', encoded)
            // Inhalt dekodieren & als Attachment loggen
            try {
                def decodedStr = new String(encoded.decodeBase64(), 'UTF-8')
                logAttachment(messageLog, 'DecodedDocument', decodedStr, 'text/plain')
            } catch (Exception e) { /* falls kein gültiger Base64 – ignorieren */ }
        } else {
            msg.setProperty('encodedDocument', '')
        }
    }

    // 5) Transformation – Hilfsfunktionen ---------------------------------------------
    def calcExternalQuoteId = { def q ->
        def comp = q.CompositeNumber.text().trim()
        def ext  = q.QuoteExternalId.text().trim()
        return (comp && ext && comp == ext) ? '' : comp
    }

    def calcCode = { def q ->
        def ext = q.QuoteExternalId.text().trim()
        return ext ? ext : q.CompositeNumber.text().trim()
    }

    def calcRank = { String rankStr ->
        int r = Math.round(rankStr.toFloat()) - 1
        return r.toString()
    }

    def calcQuantity = { String qtyStr ->
        int q = Math.round(qtyStr.toFloat())            // gem. Beispiel KEIN -1
        return q.toString()
    }

    // 6) Quote-Mapping -----------------------------------------------------------------
    def mapQuote = { def q, Message msg ->
        def sw = new StringWriter()
        new MarkupBuilder(sw).'Quotes' {
            'Quote' {
                'externalQuoteId'(calcExternalQuoteId(q))
                'externalQuoteDocument'(msg.getProperty('encodedDocument') ?: '')
                'code'(calcCode(q))
                'user' {
                    'User' {
                        'uid'(q.BillToCustomer.Email.text())
                    }
                }
                'salesOrganization'(q.SalesArea.SalesOrganization.text())
            }
        }
        return XmlUtil.serialize(sw.toString())
    }

    // 7) QuoteEntry-Mapping ------------------------------------------------------------
    def mapQuoteEntry = { def root, def item ->
        def sw = new StringWriter()
        new MarkupBuilder(sw).'QuoteEntries' {
            'QuoteEntry' {
                'order' {
                    'Quote' {
                        'code'(calcCode(root))
                    }
                }
                'rank'(calcRank(item.Rank.text()))
                'entryDiscount'(item.RolledUpDiscountAmount.DecimalValue.text())
                'basePrice'(item.ListPrice.DecimalValue.text())
                'quantity'(calcQuantity(item.Quantity.text()))
            }
        }
        return XmlUtil.serialize(sw.toString())
    }

    // 8) HTTP-Aufruf - POST (Quote & QuoteEntry) --------------------------------------
    def sendToCommerce = { String payload, Map ctx, def messageLog ->
        HttpURLConnection conn
        try {
            conn = (HttpURLConnection) (new URL(ctx.url)).openConnection()
            conn.requestMethod         = 'POST'
            conn.doOutput              = true
            conn.setRequestProperty('Authorization',
                    'Basic ' + "${ctx.user}:${ctx.pass}".getBytes('UTF-8').encodeBase64().toString())
            conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')

            conn.outputStream.withWriter('UTF-8') { it << payload }
            int rc = conn.responseCode
            logAttachment(messageLog, 'HTTP-Status', "Response-Code: ${rc}")

            if (rc >= 400) {
                throw new RuntimeException("HTTP-Fehler ${rc} beim Aufruf von ${ctx.url}")
            }
        } finally {
            conn?.disconnect()
        }
    }

    /*=================== HAUPTVERARBEITUNG ===========================================*/

    def messageLog = messageLogFactory.getMessageLog(message)
    def originalBody = message.getBody(String) ?: ''

    try {
        // Logging des eingehenden Payloads
        logAttachment(messageLog, 'IncomingPayload', originalBody, 'text/xml')

        // Kontextdaten (User/Pass/URL) lesen
        def ctx = readContextData(message)

        // Eingangs-XML parsen
        def quoteXml = new XmlSlurper().parseText(originalBody)

        // Dokument evtl. dekodieren
        decodeDocumentIfPresent(quoteXml, message, messageLog)

        // ---------------- Quote ----------------
        logAttachment(messageLog, 'BeforeQuoteMapping', XmlUtil.serialize(quoteXml))
        def quoteRequest = mapQuote(quoteXml, message)
        logAttachment(messageLog, 'AfterQuoteMapping', quoteRequest, 'text/xml')

        logAttachment(messageLog, 'BeforeSendQuote', quoteRequest, 'text/xml')
        sendToCommerce(quoteRequest, ctx, messageLog)
        logAttachment(messageLog, 'AfterSendQuote', 'Quote erfolgreich gesendet.')

        // --------------- QuoteEntries ----------
        int cnt = 0
        quoteXml.Items.Item.each { item ->
            cnt++
            logAttachment(messageLog, "BeforeQuoteEntryMapping_${cnt}", XmlUtil.serialize(item))
            def qeRequest = mapQuoteEntry(quoteXml, item)
            logAttachment(messageLog, "AfterQuoteEntryMapping_${cnt}", qeRequest, 'text/xml')

            logAttachment(messageLog, "BeforeSendQuoteEntry_${cnt}", qeRequest, 'text/xml')
            sendToCommerce(qeRequest, ctx, messageLog)
            logAttachment(messageLog, "AfterSendQuoteEntry_${cnt}", "QuoteEntry ${cnt} erfolgreich gesendet.")
        }

        // Ursprünglichen Body beibehalten
        return message

    } catch (Exception e) {
        handleError(originalBody, e, messageLog)
    }
}