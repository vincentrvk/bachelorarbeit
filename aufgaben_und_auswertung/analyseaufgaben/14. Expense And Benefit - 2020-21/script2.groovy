/*****************************************************************************************
 *  Groovy-Skript :  eFiling of Employees Payments –  UK  (Expense & Benefits)
 *  SAP Cloud Integration (CPI) –  Groovy-Script Step
 *
 *  Autor  :  ChatGPT – Senior-Developer Integration
 *  Version: 1.0
 *----------------------------------------------------------------------------------------
 *  Aufbau:
 *   1. Haupt­methoden  (processData)
 *   2. Hilfs­funktionen
 *        2.1 setHeadersAndProperties     – Header / Property-Initialisierung
 *        2.2 mapRequestPayload           – XML-Mapping (Input  ➜  Output)
 *        2.3 createMessageDigest         – SHA-1 Bildung + Einfügen IRmark
 *        2.4 writeDataStore              – Persistenz in Datastore  „EXB20-21“
 *        2.5 performSetResultsCall       – HTTP POST „Set Results“
 *        2.6 handleError                 – Zentrales Error-Handling
 *****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.*
import java.security.MessageDigest
import groovy.xml.*

Message processData(Message message) {

    // MessageLog für Monitoring  
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /***** 1. Header / Property Werte ermitteln *****/
        def context = setHeadersAndProperties(message)

        /***** 2. Eingehenden Payload mappen           *****/
        def mappedPayload = mapRequestPayload(message.getBody(String) ?: "")

        /***** 3. SHA-1 Digest bilden & IRmark einfügen *****/
        def payloadWithDigest = createMessageDigest(mappedPayload)

        /***** 4. Datastore-Write  ************************/
        writeDataStore(payloadWithDigest, context.messageId, messageLog)

        /***** 5. HTTP-Aufruf „Set Results“  **************/
        performSetResultsCall(payloadWithDigest, context, messageLog)

        // Payload ins Message-Objekt zurückschreiben
        message.setBody(payloadWithDigest)
        return message

    } catch (Exception ex) {
        // Zentrales Error-Handling
        handleError(message.getBody(String) ?: "", ex, messageLog)
        return message      // wird durch throw in handleError nie erreicht
    }
}

/*========================================================================================
 * 2.1  Header / Property Handling
 *======================================================================================*/
def setHeadersAndProperties(Message message) {
    /*
     *  Liest benötigte Header/Properties aus dem Message-Objekt.
     *  Fehlt ein Wert, wird „placeholder“ verwendet.
     */
    def context = [:]

    // Properties
    context.requestUser     = message.getProperty("requestUser")     ?: "placeholder"
    context.requestPassword = message.getProperty("requestPassword") ?: "placeholder"
    context.requestURL      = message.getProperty("requestURL")      ?: "placeholder"

    // Technische Message-ID (Header)
    context.messageId = message.getHeader("CamelMessageId", String) ?: UUID.randomUUID().toString()

    return context
}

/*========================================================================================
 * 2.2  Mapping-Funktion
 *======================================================================================*/
def mapRequestPayload(String inputXml) {
    /*
     *  Führt das definierte Mapping gemäss Vorgabe durch und erzeugt das
     *  Ziel-XML (ohne IRmark).
     */
    try {
        def input = new XmlSlurper().parseText(inputXml)

        // Namespaces
        def envNS  = "http://www.govtalk.gov.uk/CM/envelope"
        def exbNS  = "http://www.govtalk.gov.uk/taxation/EXB/20-21/1"

        def writer = new StringWriter()
        def xml    = new MarkupBuilder(writer)
        xml.mkp.xmlDeclaration(version:"1.0", encoding:"UTF-8")

        xml."GovTalkMessage"("xmlns":envNS) {
            "EnvelopeVersion"("")
            "Header" {
                "MessageDetails" {
                    "Class"("")
                    "Qualifier"("")
                }
                "SenderDetails" {
                    "IDAuthentication" {
                        "SenderID"(input.'SenderID'.text())
                        "Authentication" {
                            "Method"("")
                            "Value"(input.'Value'.text())
                        }
                    }
                }
            }
            "GovTalkDetails" {
                "ChannelRouting" {
                    "Channel" {
                        "Product"(input.'Product'.text())
                    }
                    "Timestamp"(input.'Timestamp'.text())
                }
            }
            "Body" {
                mkp.yieldUnescaped("<IRenvelope xmlns=\"${exbNS}\">")
                "IRheader" {
                    "Keys" {
                        "Key"("Type":"TaxOfficeNumber")
                        "Key"("Type":"TaxOfficeReference")
                    }
                    "PeriodEnd"("")
                    "DefaultCurrency"(input.'DefaultCurrency'.text())
                    "Sender"("")
                    // IRmark wird später eingefügt
                }
                "ExpensesAndBenefits"("")
                mkp.yieldUnescaped("</IRenvelope>")
            }
        }
        return writer.toString()

    } catch(Exception e) {
        throw new RuntimeException("Mapping fehlgeschlagen: ${e.message}", e)
    }
}

/*========================================================================================
 * 2.3  SHA-1 Digest + IRmark
 *======================================================================================*/
def createMessageDigest(String xmlPayload) {
    /*
     *  Erstellt den SHA-1 Hash über das aktuelle Payload (ohne IRmark)
     *  und fügt das Element <IRmark> an der vorgegebenen Stelle ein.
     */
    try {
        MessageDigest md = MessageDigest.getInstance("SHA-1")
        md.update(xmlPayload.getBytes("UTF-8"))
        def digestHex = md.digest().encodeHex().toString()

        // Einfügen des Elements –  Parsing & Modifikation
        def parsed = new XmlParser(false, false).parseText(xmlPayload)
        def envNS  = new groovy.xml.Namespace("http://www.govtalk.gov.uk/CM/envelope","")
        def exbNS  = new groovy.xml.Namespace("http://www.govtalk.gov.uk/taxation/EXB/20-21/1","")

        def irHeader = parsed."${envNS.Body}"."${exbNS.IRenvelope}"."${exbNS.IRheader}"[0]
        irHeader.appendNode("IRmark", digestHex)

        // Serialisieren
        def sw = new StringWriter()
        XmlNodePrinter printer = new XmlNodePrinter(new PrintWriter(sw))
        printer.with {
            preserveWhitespace = true
        }
        printer.print(parsed)
        return sw.toString()

    } catch(Exception e) {
        throw new RuntimeException("SHA-1 Bildung fehlgeschlagen: ${e.message}", e)
    }
}

/*========================================================================================
 * 2.4  Datastore-Write
 *======================================================================================*/
def writeDataStore(String payload, String entryId, def messageLog) {
    /*
     *  Schreibt den finalen Payload in den Datastore  „EXB20-21“
     *  Key = MessageID    | Overwrite = true
     */
    def service = new Factory(DataStoreService.class).getService()
    if (service == null) {
        messageLog?.addAttachmentAsString("DatastoreWarning", "Service Instanz konnte nicht ermittelt werden.", "text/plain")
        return
    }
    try {
        DataBean  dataBean  = new DataBean()
        dataBean.setDataAsArray(payload.getBytes("UTF-8"))

        DataConfig cfg = new DataConfig()
        cfg.setStoreName("EXB20-21")
        cfg.setId(entryId)
        cfg.setOverwrite(true)

        service.put(dataBean, cfg)

    } catch(Exception e) {
        throw new RuntimeException("Datastore-Write fehlgeschlagen: ${e.message}", e)
    }
}

/*========================================================================================
 * 2.5  HTTP-Call  „Set Results“
 *======================================================================================*/
def performSetResultsCall(String payload, def ctx, def messageLog) {
    /*
     *  Führt den POST-Aufruf an die im Property hinterlegte URL durch.
     *  Authentifizierung über Basic-Auth.
     */
    try {
        def urlConn = new URL(ctx.requestURL).openConnection()
        urlConn.setRequestMethod("POST")
        urlConn.setDoOutput(true)

        def auth = "${ctx.requestUser}:${ctx.requestPassword}".bytes.encodeBase64().toString()
        urlConn.setRequestProperty("Authorization", "Basic ${auth}")
        urlConn.setRequestProperty("Content-Type", "application/xml; charset=UTF-8")

        urlConn.outputStream.withWriter("UTF-8") { it << payload }

        def rc = urlConn.responseCode
        messageLog?.setStringProperty("SetResults-HTTPStatus", rc.toString())

        if(rc >= 400) {
            def err = urlConn.errorStream?.getText("UTF-8") ?: ""
            throw new RuntimeException("HTTP Fehlercode ${rc}. Antwort: ${err}")
        }

    } catch(Exception e) {
        throw new RuntimeException("HTTP-Call Fehlgeschlagen: ${e.message}", e)
    }
}

/*========================================================================================
 * 2.6  Zentrales Error-Handling
 *======================================================================================*/
def handleError(String body, Exception e, def messageLog) {
    /*
     *  Fügt den fehlerhaften Payload als Attachment hinzu
     *  und wirft eine RuntimeException zwecks CPI Exception-Handling.
     */
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}