/*****************************************************************************************
 *  Name      : EPS_EmployerPaymentSummary_21_22.groovy
 *  Purpose   : Verarbeitung von EPS-Nachrichten (UK) – Mapping, Hash-Bildung, Datastore,
 *              Aufruf des HMRC-Endpoints.
 *  Autor     : AI Senior-Developer (SAP CPI, Groovy)
 *****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory
import groovy.xml.MarkupBuilder
import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.net.URL

/******************************** Main ***************************************************/
Message processData(Message message) {
    def msgLog = messageLogFactory.getMessageLog(message)

    try {
        // 1) Header & Property Handling
        def cfg = setHeadersAndProperties(message, msgLog)

        // 2) Eingehendes Payload in Datastore schreiben
        writeDatastore(message, msgLog)

        // 3) Mapping – Eingabe  ➜  GovTalkMessage ohne IRmark
        String inboundPayload = message.getBody(String) ?: ''
        def inData          = extractInputData(inboundPayload, msgLog)
        String xmlNoDigest  = buildGovTalkMessage(inData, null)

        // 4) SHA-1 Hash über Payload (ohne IRmark) bilden
        String digestValue  = calculateMessageDigest(xmlNoDigest)

        // 5) Endgültiges Payload (inkl. IRmark) erzeugen
        String finalPayload = buildGovTalkMessage(inData, digestValue)
        message.setBody(finalPayload)

        // 6) HMRC-Endpoint aufrufen (POST)
        callEndpoint(cfg.url, cfg.user, cfg.password, finalPayload, msgLog)

    } catch (Exception ex) {
        handleError(message.getBody(String) ?: '', ex, msgLog)
    }

    return message
}

/****************************** Modul-Funktionen *****************************************/

/*  Legt Properties falls nicht vorhanden und liefert Konfig zurück                                          */
def Map setHeadersAndProperties(Message message, def msgLog) {
    def props    = message.getProperties()
    def user     = props['requestUser']     ?: 'placeholder'
    def password = props['requestPassword'] ?: 'placeholder'
    def url      = props['requestURL']      ?: 'placeholder'

    message.setProperty('requestUser'    , user)
    message.setProperty('requestPassword', password)
    message.setProperty('requestURL'     , url)

    return [user: user, password: password, url: url]
}

/*  Persistiert das eingehende Payload im DataStore "EPS21-22"                                               */
def writeDatastore(Message message, def msgLog) {
    try {
        def service = new Factory(DataStoreService.class).getService()
        if (service) {
            def bean   = new DataBean()
            bean.setDataAsArray((message.getBody(String) ?: '').getBytes(StandardCharsets.UTF_8))

            def cfg = new DataConfig()
            cfg.setStoreName('EPS21-22')
            cfg.setId(message.getHeader('CamelMessageId', String.class) ?: UUID.randomUUID().toString())
            cfg.setOverwrite(true)

            service.put(bean, cfg)
        } else {
            msgLog?.addAttachmentAsString('DatastoreWarning',
                    'DataStoreService konnte nicht instanziiert werden – Eintrag wird übersprungen.',
                    'text/plain')
        }
    } catch (Exception e) {
        handleError(message.getBody(String) ?: '', e, msgLog)
    }
}

/*  Zerlegt Eingangspayload in Map (Felder gem. Mapping-Anforderung)                                         */
def Map extractInputData(String xml, def msgLog) {
    try {
        def root = new XmlSlurper().parseText(xml)
        return [
                senderID : root.SenderID.text(),
                value    : root.Value.text(),
                product  : root.Product.text(),
                timestamp: root.Timestamp.text(),
                periodEnd: root.PeriodEnd.text()
        ]
    } catch (Exception e) {
        handleError(xml, e, msgLog)
        return [:]  // wird durch handleError nicht mehr erreicht, Compiler-Beruhigung
    }
}

/*  Erzeugt GovTalkMessage-XML – digest == null ➜ IRmark wird nicht geschrieben                              */
def String buildGovTalkMessage(Map data, String digest) {
    StringWriter sw = new StringWriter()
    def xml = new MarkupBuilder(sw)

    xml.GovTalkMessage('xmlns': 'http://www.govtalk.gov.uk/CM/envelope') {
        EnvelopeVersion('')
        Header {
            MessageDetails {
                Class('')
                Qualifier('')
            }
            SenderDetails {
                IDAuthentication {
                    SenderID(data.senderID)
                    Authentication {
                        Method('')
                        Value(data.value)
                    }
                }
            }
        }
        GovTalkDetails {
            ChannelRouting {
                Channel {
                    Product(data.product)
                }
                Timestamp(data.timestamp)
            }
        }
        Body {
            IRenvelope('xmlns': 'http://www.govtalk.gov.uk/taxation/PAYE/RTI/EmployerPaymentSummary/21-22/1') {
                IRheader {
                    Keys {
                        Key('Type': 'TaxOfficeNumber')
                        Key('Type': 'TaxOfficeReference')
                    }
                    if (digest) { IRmark(digest) }
                    PeriodEnd(data.periodEnd)
                    Sender('')
                }
                EmployerPaymentSummary {
                    RelatedTaxYear('')
                }
            }
        }
    }
    return sw.toString()
}

/*  SHA-1 Digest (hex-String)                                                                                */
def String calculateMessageDigest(String payload) {
    MessageDigest md = MessageDigest.getInstance('SHA-1')
    byte[] hash      = md.digest(payload.getBytes(StandardCharsets.UTF_8))
    return hash.encodeHex().toString()
}

/*  HTTP POST-Aufruf des konfigurierten Endpoints                                                            */
def callEndpoint(String url, String user, String pwd, String payload, def msgLog) {
    try {
        if ('placeholder'.equalsIgnoreCase(url)) {
            msgLog?.addAttachmentAsString('EndpointSkip',
                    'Property requestURL == placeholder – HTTP-Aufruf übersprungen.', 'text/plain')
            return
        }

        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
        conn.setRequestMethod('POST')
        conn.setDoOutput(true)
        conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
        String basicAuth = "${user}:${pwd}".bytes.encodeBase64().toString()
        conn.setRequestProperty('Authorization', "Basic ${basicAuth}")

        conn.outputStream.withWriter('UTF-8') { it << payload }

        int respCode = conn.getResponseCode()
        msgLog?.addAttachmentAsString('HTTP-Response-Code', respCode.toString(), 'text/plain')

        conn.disconnect()
    } catch (Exception e) {
        handleError(payload, e, msgLog)
    }
}

/*  Zentrales Error-Handling – schreibt Payload als Attachment & wirft RuntimeException                      */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im EPS-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}