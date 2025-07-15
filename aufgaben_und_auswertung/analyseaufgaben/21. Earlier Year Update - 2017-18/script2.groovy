/******************************************************************************
 * Groovy-Skript – Earlier Year Update (EYU) UK – eFiling of Employees Payments
 * ----------------------------------------------------------------------------
 * Dieses Skript erfüllt folgende Aufgaben:
 * 1. Lesen von Properties & Headern oder Setzen von Default-Platzhaltern
 * 2. Mapping des eingehenden Payloads auf das Zielschema
 * 3. Bilden eines SHA-1-Message-Digest und Einfügen in das Ergebnis-XML
 * 4. Schreiben des Payloads in einen Datastore (Name: “EYU17-18”)
 * 5. Aufbereitung des Messages für den nachfolgenden HTTP-POST-Aufruf
 *
 * Alle Funktionen sind modular implementiert, enthalten deutschsprachige
 * Kommentare und besitzen aussagekräftiges Error-Handling.
 ******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.*
import java.security.MessageDigest
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory

/* ============================================================
 * Haupt-Einstiegspunkt
 * ============================================================
 */
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    try {

        /* 1. Properties & Header verarbeiten */
        setHeaderAndPropertyValues(message)

        /* 2. Eingehenden Payload lesen */
        String incomingXml = message.getBody(String) as String

        /* 3. Mapping ausführen */
        String mappedXml = buildMappedPayload(incomingXml)

        /* 4. Digest berechnen & einfügen */
        String finalXml = insertMessageDigest(mappedXml)

        /* 5. Datastore-Write durchführen */
        String entryId = (message.getHeaders()["CamelMessageId"] ?: UUID.randomUUID().toString()) as String
        writePayloadToDataStore(finalXml, entryId)

        /* 6. Ergebnis in den Message-Body legen */
        message.setBody(finalXml)

        return message

    } catch (Exception e) {
        /* Zentrales Error-Handling */
        handleError(message.getBody(String) as String, e, messageLog)
        return message   // wird nie erreicht, handleError wirft Exception
    }
}

/* ============================================================
 * Funktion: Header & Properties verarbeiten
 * ============================================================
 */
def setHeaderAndPropertyValues(Message message) {
    /*
     * Prüft, ob requestUser, requestPassword und requestURL bereits gesetzt
     * sind. Falls nicht, werden Platzhalter-Werte verwendet.
     * Zusätzlich wird ein HTTP-Authorization-Header aufgebaut.
     */
    def props = message.getProperties()
    String user = props.requestUser ?: "placeholder"
    String pwd  = props.requestPassword ?: "placeholder"
    String url  = props.requestURL ?: "placeholder"

    message.setProperty("requestUser",     user)
    message.setProperty("requestPassword", pwd)
    message.setProperty("requestURL",      url)

    String auth = (user + ":" + pwd).getBytes("UTF-8").encodeBase64().toString()
    message.setHeader("Authorization", "Basic ${auth}")
    message.setHeader("CamelHttpMethod", "POST")          // HTTP-Methode
}

/* ============================================================
 * Funktion: Mapping auf Ziel-XML
 * ============================================================
 */
def buildMappedPayload(String inputXml) {
    /*
     * Erzeugt das Ziel-XML gemäß der Mapping-Anforderungen.
     */
    def slurper = new XmlSlurper().parseText(inputXml)

    StringWriter writer = new StringWriter()
    def xml = new MarkupBuilder(writer)

    xml.GovTalkMessage(xmlns: "http://www.govtalk.gov.uk/CM/envelope") {
        EnvelopeVersion()
        Header {
            MessageDetails {
                Class()
                Qualifier()
                CorrelationID()
                GatewayTimestamp()
            }
            SenderDetails {
                IDAuthentication {
                    SenderID(slurper.SenderID.text())
                    Authentication {
                        Method()
                        Value(slurper.Value.text())
                    }
                }
            }
        }
        GovTalkDetails {
            ChannelRouting {
                Channel {
                    Product(slurper.Product.text())
                }
                Timestamp(slurper.Timestamp.text())
            }
        }
        Body {
            IRenvelope('xmlns': "http://www.govtalk.gov.uk/taxation/PAYE/RTI/EarlierYearUpdate/16-17/1") {
                IRheader {
                    Keys {
                        Key(Type: "TaxOfficeNumber")
                        Key(Type: "TaxOfficeReference")
                    }
                    PeriodEnd(slurper.PeriodEnd.text())
                    Sender()
                    /* IRmark wird später eingefügt */
                }
                EarlierYearUpdate {
                    RelatedTaxYear()
                }
            }
        }
    }
    return writer.toString()
}

/* ============================================================
 * Funktion: SHA-1 Digest berechnen & einfügen
 * ============================================================
 */
def insertMessageDigest(String xmlWithoutDigest) {
    /*
     * Berechnet den SHA-1-Digest des übergebenen XMLs (ohne IRmark)
     * und fügt das Ergebnis als <IRmark> in den geforderten Pfad ein.
     */
    String digest = computeSha1(xmlWithoutDigest)

    def parser = new XmlParser()
    def root   = parser.parseText(xmlWithoutDigest)
    def irHeader = root.Body[0].IRenvelope[0].IRheader[0]

    /* Vorhandene IRmark-Elemente entfernen */
    irHeader.children().findAll { it.name() == "IRmark" }.each { irHeader.remove(it) }

    /* Neues IRmark-Element anhängen */
    irHeader.appendNode("IRmark", digest)

    /* Serialisieren mit XML-Header */
    return groovy.xml.XmlUtil.serialize(root)
}

/* ============================================================
 * Funktion: SHA-1 Digest erzeugen
 * ============================================================
 */
def computeSha1(String text) {
    /*
     * Berechnet einen SHA-1-Hex-String für den übergebenen Text.
     */
    MessageDigest md = MessageDigest.getInstance("SHA-1")
    byte[] hashBytes = md.digest(text.getBytes("UTF-8"))
    return hashBytes.encodeHex().toString()
}

/* ============================================================
 * Funktion: Datastore-Write
 * ============================================================
 */
def writePayloadToDataStore(String payload, String entryId) {
    /*
     * Schreibt das übergebene Payload als Byte-Array in den Datastore
     * “EYU17-18”. Das Entry wird anhand des CamelMessageId-Headers
     * (oder einer generierten UUID) indiziert.
     */
    def service = new Factory(DataStoreService.class).getService()

    if (service) {
        DataBean dataBean = new DataBean()
        dataBean.setDataAsArray(payload.getBytes("UTF-8"))

        DataConfig cfg = new DataConfig()
        cfg.setStoreName("EYU17-18")
        cfg.setId(entryId)
        cfg.setOverwrite(true)

        service.put(dataBean, cfg)
    } else {
        throw new RuntimeException("DataStoreService konnte nicht initialisiert werden.")
    }
}

/* ============================================================
 * Funktion: Zentrales Error-Handling
 * ============================================================
 */
def handleError(String body, Exception e, def messageLog) {
    /*
     * Fügt den fehlerhaften Payload als Attachment hinzu und wirft
     * anschließend eine RuntimeException mit detaillierter Meldung.
     */
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "", "text/xml")
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}