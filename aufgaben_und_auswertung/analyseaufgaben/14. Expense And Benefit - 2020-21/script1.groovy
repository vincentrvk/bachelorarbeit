/*****************************************************************************
 *  eFiling of Employees Payments UK – Expense & Benefits
 *  Groovy-Skript für SAP Cloud Integration
 *
 *  Anforderungen:
 *  1. Modularer Aufbau (eigene Funktionen je Schritt)
 *  2. Deutsche Kommentare
 *  3. Umfassendes Error-Handling inkl. Attachment (siehe handleError)
 *  4. SHA-1-Digest (IRmark) erzeugen und einfügen
 *  5. Payload in DataStore „EXB20-21“ ablegen
 *  6. HTTP-POST (Set Results) mit Basic-Auth ausführen
 *****************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory
import java.security.MessageDigest
import java.net.URL
import groovy.xml.MarkupBuilder

/* ------------------------------------------------------------ *
 *                  ZENTRALES ERROR-HANDLING                    *
 * ------------------------------------------------------------ */

/**
 * Einheitliches Error-Handling.
 * Hängt den fehlerhaften Payload als Attachment an das MPL
 * und wirft anschließend eine RuntimeException.
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "", "text/xml")
    def errorMsg = "Fehler im Groovy-Skript: ${e.getMessage()}"
    throw new RuntimeException(errorMsg, e)
}

/* ------------------------------------------------------------ *
 *               HEADER / PROPERTIES – VORBEREITEN              *
 * ------------------------------------------------------------ */

/**
 * Legt benötigte Properties an (requestUser, requestPassword, requestURL),
 * sofern sie noch nicht existieren. Fehlende Werte werden auf „placeholder“
 * gesetzt, damit Folge-Schritte nie null erhalten.
 */
def setContextValues(Message message, def messageLog) {
    try {
        ['requestUser', 'requestPassword', 'requestURL'].each { key ->
            if (message.getProperty(key) == null) {
                message.setProperty(key, 'placeholder')
            }
        }
    } catch (Exception e) {
        handleError(message.getBody(String) as String, e, messageLog)
    }
}

/* ------------------------------------------------------------ *
 *                       REQUEST MAPPING                        *
 * ------------------------------------------------------------ */

/**
 * Erstellt aus dem Eingangs-XML das gewünschte Ziel-XML.
 * Namespace-Aware über MarkupBuilder umgesetzt.
 */
def buildMappedPayload(String sourceXml, def messageLog) {
    try {
        def src = new XmlSlurper().parseText(sourceXml)

        def writer = new StringWriter()
        def mb = new MarkupBuilder(writer)

        mb.'GovTalkMessage'('xmlns': 'http://www.govtalk.gov.uk/CM/envelope') {
            'EnvelopeVersion'('')
            'Header' {
                'MessageDetails' {
                    'Class'('')
                    'Qualifier'('')
                }
                'SenderDetails' {
                    'IDAuthentication' {
                        'SenderID'(src.SenderID.text())
                        'Authentication' {
                            'Method'('')
                            'Value'(src.Value.text())
                        }
                    }
                }
            }
            'GovTalkDetails' {
                'ChannelRouting' {
                    'Channel' {
                        'Product'(src.Product.text())
                    }
                    'Timestamp'(src.Timestamp.text())
                }
            }
            'Body' {
                'IRenvelope'('xmlns': 'http://www.govtalk.gov.uk/taxation/EXB/20-21/1') {
                    'IRheader' {
                        'Keys' {
                            'Key'('Type': 'TaxOfficeNumber')
                            'Key'('Type': 'TaxOfficeReference')
                        }
                        'PeriodEnd'('')
                        'DefaultCurrency'(src.DefaultCurrency.text())
                        'Sender'('')
                        /* IRmark kommt im nächsten Schritt */
                    }
                    'ExpensesAndBenefits'('')
                }
            }
        }
        return writer.toString()
    } catch (Exception e) {
        handleError(sourceXml, e, messageLog)
    }
}

/* ------------------------------------------------------------ *
 *                  MESSAGE-DIGEST (IRmark)                     *
 * ------------------------------------------------------------ */

/**
 * Berechnet den SHA-1-Digest über das übergebene XML (UTF-8)
 * und fügt den Wert als <IRmark> innerhalb von <IRheader> ein.
 */
def generateDigestAndInsert(String xmlPayload, def messageLog) {
    try {
        MessageDigest md = MessageDigest.getInstance('SHA-1')
        byte[] digestBytes = md.digest(xmlPayload.getBytes('UTF-8'))
        String digestHex = digestBytes.collect { String.format('%02x', it) }.join()

        // IRmark direkt per String-Replacement einfügen, um Namespaces zu schonen
        if (!xmlPayload.contains('</IRheader>')) {
            throw new IllegalStateException('IRheader-Element nicht gefunden – IRmark kann nicht eingefügt werden.')
        }
        String enrichedXml = xmlPayload.replaceFirst('</IRheader>', "<IRmark>${digestHex}</IRmark></IRheader>")
        return enrichedXml
    } catch (Exception e) {
        handleError(xmlPayload, e, messageLog)
    }
}

/* ------------------------------------------------------------ *
 *                      DATASTORE WRITE                         *
 * ------------------------------------------------------------ */

/**
 * Persistiert den Payload im DataStore „EXB20-21“ unter der
 * EntryId = MessageID des MPL (Fallback: UUID).
 */
def writeToDatastore(String payload, Message message, def messageLog) {
    try {
        def service = new Factory(DataStoreService.class).getService()
        if (service == null) {
            throw new IllegalStateException('DataStoreService konnte nicht instanziiert werden.')
        }

        def dataBean = new DataBean()
        dataBean.setDataAsArray(payload.getBytes('UTF-8'))

        def config = new DataConfig()
        config.setStoreName('EXB20-21')

        // MessageID ermitteln (Header bevorzugt, sonst Property, sonst UUID)
        def entryId = message.getHeader('MessageID', String)
        if (!entryId) entryId = message.getProperty('SAP_MessageProcessingLogID') as String
        if (!entryId) entryId = UUID.randomUUID().toString()
        config.setId(entryId)

        config.setOverwrite(true)
        service.put(dataBean, config)
    } catch (Exception e) {
        handleError(payload, e, messageLog)
    }
}

/* ------------------------------------------------------------ *
 *                  HTTP-CALL  (SET RESULTS)                    *
 * ------------------------------------------------------------ */

/**
 * Führt einen HTTP-POST gegen ${requestURL} aus.
 * Body = Payload, Auth = Basic (requestUser:requestPassword)
 * Es erfolgt kein spezielles Response-Handling, Statuscode wird geloggt.
 */
def callSetResultsApi(String payload, Message message, def messageLog) {
    try {
        String urlStr = message.getProperty('requestURL') as String
        String user   = message.getProperty('requestUser') as String
        String pwd    = message.getProperty('requestPassword') as String

        URL url = new URL(urlStr)
        def conn = url.openConnection()
        conn.setRequestMethod('POST')
        conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
        String auth = "${user}:${pwd}".bytes.encodeBase64().toString()
        conn.setRequestProperty('Authorization', "Basic ${auth}")
        conn.setDoOutput(true)

        conn.getOutputStream().withWriter('UTF-8') { it << payload }
        int rc = conn.getResponseCode()
        messageLog?.addAttachmentAsString('HTTP-Status', rc.toString(), 'text/plain')
    } catch (Exception e) {
        handleError(payload, e, messageLog)
    }
}

/* ------------------------------------------------------------ *
 *                       HAUPTPROZESS                           *
 * ------------------------------------------------------------ */

Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    def incoming   = message.getBody(String) ?: ''

    /* 1) Header/Properties sicherstellen */
    setContextValues(message, messageLog)

    /* 2) Mapping durchführen */
    def mappedXml = buildMappedPayload(incoming, messageLog)

    /* 3) SHA-1-Digest erzeugen und einfügen */
    def finalXml  = generateDigestAndInsert(mappedXml, messageLog)

    /* 4) Payload in DataStore schreiben */
    writeToDatastore(finalXml, message, messageLog)

    /* 5) HTTP-POST an „Set Results“ */
    callSetResultsApi(finalXml, message, messageLog)

    /* 6) Ergebnis als Message-Body zurückgeben */
    message.setBody(finalXml)
    return message
}