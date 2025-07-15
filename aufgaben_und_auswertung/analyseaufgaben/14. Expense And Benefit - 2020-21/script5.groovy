/****************************************************************************************
*  SAP Cloud Integration – Groovy-Skript
*  Aufgabe:      eFiling of Employees Payments (UK) – Expenses & Benefits Import
*  Autor:        ChatGPT (Senior Integration Developer)
*  Beschreibung: Erstellt den Request-Payload, ergänzt SHA-1-Digest, schreibt Payload
*                in einen DataStore und bereitet den HTTP-Aufruf vor.
*****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import java.security.MessageDigest
import groovy.xml.MarkupBuilder
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory

/* =========================================
 *  Main Processing
 * =======================================*/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        // 1. Header / Property Handling
        def meta = setHeadersAndProperties(message)

        // 2. Mapping des eingehenden Payloads
        def sourcePayload = message.getBody(String) as String
        def mappedPayload = performMapping(sourcePayload)

        // 3. SHA-1 IRmark hinzufügen
        def payloadWithDigest = addMessageDigest(mappedPayload)

        // 4. DataStore Write
        writeToDataStore(payloadWithDigest, message)

        // 5. Finalen Payload in Message-Body setzen
        message.setBody(payloadWithDigest)
        return message

    } catch (Exception e) {
        handleError(message.getBody(String) as String, e, messageLog)
    }
}


/* =========================================
 *  Funktion: setHeadersAndProperties
 * -----------------------------------------
 *  Ermittelt benötigte Properties / Header
 *  und legt fehlende Werte als „placeholder“
 *  auf der Message ab.
 * =======================================*/
def setHeadersAndProperties(Message message) {

    // vorhandene Werte ermitteln oder 'placeholder' setzen
    String reqUser     = (message.getProperty('requestUser')     ?: message.getHeader('requestUser',     String.class)) ?: 'placeholder'
    String reqPassword = (message.getProperty('requestPassword') ?: message.getHeader('requestPassword', String.class)) ?: 'placeholder'
    String reqURL      = (message.getProperty('requestURL')      ?: message.getHeader('requestURL',      String.class)) ?: 'placeholder'

    // Properties sicherstellen
    message.setProperty('requestUser',     reqUser)
    message.setProperty('requestPassword', reqPassword)
    message.setProperty('requestURL',      reqURL)

    // HTTP-Header für Basic Auth setzen (für Receiver-Aufruf)
    String authHeader = "${reqUser}:${reqPassword}".bytes.encodeBase64().toString()
    message.setHeader('Authorization', "Basic ${authHeader}")
    message.setHeader('Content-Type', 'application/xml')
    message.setHeader('CamelHttpMethod', 'POST')          // für HTTP Adapter

    return [user:reqUser, password:reqPassword, url:reqURL]
}


/* =========================================
 *  Funktion: performMapping
 * -----------------------------------------
 *  Erstellt den Ziel-XML-Payload gem. Mapping
 *  – ohne IRmark, dieser wird später ergänzt.
 * =======================================*/
def performMapping(String sourceXml) {

    // Eingangs-XML parsen
    def input = new XmlSlurper().parseText(sourceXml)

    // Ziel-XML erzeugen
    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)

    xml.'GovTalkMessage'('xmlns':'http://www.govtalk.gov.uk/CM/envelope') {
        'EnvelopeVersion'()
        'Header'{
            'MessageDetails'{
                'Class'()
                'Qualifier'()
            }
            'SenderDetails'{
                'IDAuthentication'{
                    'SenderID'(input.SenderID.text())
                    'Authentication'{
                        'Method'()
                        'Value'(input.Value.text())
                    }
                }
            }
        }
        'GovTalkDetails'{
            'ChannelRouting'{
                'Channel'{
                    'Product'(input.Product.text())
                }
                'Timestamp'(input.Timestamp.text())
            }
        }
        'Body'{
            'IRenvelope'('xmlns':'http://www.govtalk.gov.uk/taxation/EXB/20-21/1'){
                'IRheader'{
                    // IRmark wird später eingefügt
                    'Keys'{
                        'Key'('Type':'TaxOfficeNumber')
                        'Key'('Type':'TaxOfficeReference')
                    }
                    'PeriodEnd'()
                    'DefaultCurrency'(input.DefaultCurrency.text())
                    'Sender'()
                }
                'ExpensesAndBenefits'()
            }
        }
    }
    return sw.toString()
}


/* =========================================
 *  Funktion: addMessageDigest
 * -----------------------------------------
 *  Berechnet SHA-1 Digest des aktuellen
 *  Payloads und fügt den Wert als <IRmark>
 *  in den Payload ein.
 * =======================================*/
def addMessageDigest(String payload) {

    // SHA-1 berechnen
    String digest = sha1Hex(payload)

    // IRmark nach dem öffnenden <IRheader> Tag einfügen
    String updated = payload.replaceFirst(/<IRheader>/,
                    '<IRheader><IRmark>' + digest + '</IRmark>')

    return updated
}


/* =========================================
 *  Funktion: sha1Hex
 * -----------------------------------------
 *  Liefert SHA-1 des übergebenen Strings als
 *  Hex-String.
 * =======================================*/
def sha1Hex(String value) {

    MessageDigest md = MessageDigest.getInstance('SHA-1')
    md.update(value.getBytes('UTF-8'))
    byte[] digestBytes = md.digest()

    // Bytes -> Hex-String
    digestBytes.collect { String.format('%02x', it) }.join()
}


/* =========================================
 *  Funktion: writeToDataStore
 * -----------------------------------------
 *  Schreibt Payload in DataStore „EXB20-21“
 *  unter EntryId = Message-ID.
 * =======================================*/
def writeToDataStore(String payload, Message message) {

    // Message-ID (falls nicht vorhanden, UUID generieren)
    String msgId = message.getHeaders().get('SAP_MessageProcessingId') ?: UUID.randomUUID().toString()

    def service = new Factory(DataStoreService.class).getService()
    if (service == null) {
        throw new RuntimeException('DataStoreService konnte nicht initialisiert werden.')
    }

    // DataBean erstellen
    DataBean bean = new DataBean()
    bean.setDataAsArray(payload.getBytes('UTF-8'))

    // Konfiguration setzen
    DataConfig cfg = new DataConfig()
    cfg.setStoreName('EXB20-21')
    cfg.setId(msgId)
    cfg.setOverwrite(true)

    // Schreiben
    service.put(bean, cfg)
}


/* =========================================
 *  Funktion: handleError
 * -----------------------------------------
 *  Zentrales Error-Handling gem. Vorgabe.
 *  Fügt Payload als Attachment hinzu und
 *  wirft die Exception erneut.
 * =======================================*/
def handleError(String body, Exception e, def messageLog) {
    // Payload als Attachment für Monitoring
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}