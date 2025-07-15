/****************************************************************************************
 *  Groovy-Skript:  eFiling FPS 21-22 – Request Aufbereitung
 *  Beschreibung  :  – Mapping des eingehenden Payloads
 *                   – Berechnung & Einfügen des SHA-1-IRmark
 *                   – Schreiben des Payloads in den Datastore „FPS21-22“
 *                   – Versenden des Payloads per HTTP-POST (Basic Auth)
 *  Autor         :  CPI-Integration – Senior Developer
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import java.security.MessageDigest
import groovy.xml.MarkupBuilder
import groovy.xml.XmlParser
import groovy.xml.XmlUtil
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory

// ============================== HAUPTEINSTIEG =========================================
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    def originalBody = message.getBody(String) ?: ''

    try {

        /* 1. Header / Property Werte bestimmen */
        def (requestURL, requestUser, requestPassword) = resolveConnectionData(message)

        /* 2. Mapping durchführen */
        String mappedXml = createRequestMapping(originalBody)

        /* 3. SHA-1-Digest bilden & einfügen */
        String xmlWithDigest = addIRmark(mappedXml)

        /* 4. Payload im Datastore sichern */
        writeToDatastore(xmlWithDigest, determineEntryId(message))

        /* 5. HTTP-POST an HMRC ausführen (keine Response-Verarbeitung nötig) */
        callSetResultsAPI(requestURL, requestUser, requestPassword, xmlWithDigest, messageLog)

        /* 6. Angepassten Body an Folgeschritte übergeben */
        message.setBody(xmlWithDigest)
        return message

    } catch (Exception ex) {
        handleError(originalBody, ex, messageLog)   // wirft RuntimeException
    }
}

/*=======================================================================================
 *                               H I L F S F U N K T I O N E N
 =======================================================================================*/

/* Error-Handling – wirft RuntimeException und hängt den fehlerhaften Payload an */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    throw new RuntimeException("Fehler im FPS-Skript: ${e.message}", e)
}

/* Ermittelt URL, User, Password aus Properties/Headers oder setzt „placeholder“ */
def resolveConnectionData(Message msg) {
    def url      = msg.getProperty('requestURL')     ?: msg.getHeader('requestURL',     String) ?: 'placeholder'
    def user     = msg.getProperty('requestUser')    ?: msg.getHeader('requestUser',    String) ?: 'placeholder'
    def password = msg.getProperty('requestPassword')?: msg.getHeader('requestPassword',String) ?: 'placeholder'
    return [url, user, password]
}

/* Erstellt das komplette Request-XML laut Mapping-Vorgabe                                     */
String createRequestMapping(String inputXml) {

    def source = new XmlParser().parseText(inputXml)
    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)

    xml.GovTalkMessage(xmlns: 'http://www.govtalk.gov.uk/CM/envelope') {
        EnvelopeVersion()
        Header {
            MessageDetails {
                Class()
                Qualifier()
            }
            SenderDetails {
                IDAuthentication {
                    SenderID(source.SenderID.text())
                    Authentication {
                        Method()
                        Value(source.Value.text())
                    }
                }
            }
        }
        GovTalkDetails {
            ChannelRouting {
                Channel {
                    Product(source.Product.text())
                }
                Timestamp(source.Timestamp.text())
            }
        }
        Body {
            IRenvelope(xmlns: 'http://www.govtalk.gov.uk/taxation/PAYE/RTI/FullPaymentSubmission/21-22/1') {
                IRheader {
                    Keys {
                        Key(Type: 'TaxOfficeNumber')
                        Key(Type: 'TaxOfficeReference')
                    }
                    PeriodEnd(source.PeriodEnd.text())
                    Sender()
                    /* IRmark wird in addIRmark() ergänzt */
                }
                FullPaymentSubmission {
                    RelatedTaxYear()
                }
            }
        }
    }
    return sw.toString()
}

/* Berechnet SHA-1 über das aktuelle XML und fügt den IRmark unter IRheader ein */
String addIRmark(String xml) {

    // SHA-1 Digest hexadezimal berechnen
    byte[] digestBytes = MessageDigest.getInstance("SHA-1").digest(xml.getBytes("UTF-8"))
    String digestHex   = digestBytes.collect { String.format("%02x", it) }.join()

    // XML parsen und IRmark anhängen
    def root = new XmlParser().parseText(xml)
    root.Body.IRenvelope.IRheader[0].appendNode('IRmark', digestHex)

    return XmlUtil.serialize(root)
}

/* Schreibt den Payload in den Datastore „FPS21-22“ unter der Message-ID */
void writeToDatastore(String payload, String entryId) {

    def service = new Factory(DataStoreService.class).getService()
    if (service == null) {
        throw new RuntimeException('DataStoreService konnte nicht initialisiert werden.')
    }
    def bean   = new DataBean()
    bean.setDataAsArray(payload.getBytes('UTF-8'))

    def cfg = new DataConfig()
    cfg.setStoreName('FPS21-22')
    cfg.setId(entryId)
    cfg.setOverwrite(true)

    service.put(bean, cfg)
}

/* Führt den HTTP-POST gegen HMRC aus – ohne Response-Verarbeitung */
void callSetResultsAPI(String url, String user, String pwd, String payload, def log) {

    if ('placeholder'.equals(url)) {                       // keine echte Konfiguration vorhanden
        log?.addAttachmentAsString('SkipCall', 'URL placeholder – HTTP-Aufruf übersprungen', 'text/plain')
        return
    }

    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    conn.setRequestMethod('POST')
    def auth = "${user}:${pwd}".getBytes('UTF-8').encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${auth}")
    conn.setRequestProperty('Content-Type', 'application/xml')
    conn.setDoOutput(true)

    conn.outputStream.withWriter('UTF-8') { it << payload }
    conn.connect()

    /* Loggen des Response-Codes rein informativ */
    log?.setStringProperty('HMRC_ResponseCode', "${conn.responseCode}")
    conn.inputStream?.close()
    conn.disconnect()
}

/* Liefert eine eindeutige Entry-ID (Message-ID) für den Datastore */
String determineEntryId(Message msg) {
    return msg.getHeader('SAP_MessageProcessingLogID', String) ?:
           msg.getHeader('CamelID', String) ?:
           java.util.UUID.randomUUID().toString()
}