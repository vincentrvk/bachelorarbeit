/****************************************************************************************
 *  Full-Payment-Submission (FPS) – eFiling UK (21-22)                                    *
 *  Groovy-Script für SAP Cloud Integration                                              *
 *                                                                                      *
 *  Autor  : ChatGPT (Senior-Integrator)                                                *
 *  Version: 1.0                                                                        *
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.runtime.Factory
import com.sap.it.api.asdk.datastore.*
import java.security.MessageDigest
import java.util.Base64
import groovy.xml.*


/* =========================================================================
 *  Einstiegspunkt der Script-Ausführung
 * -------------------------------------------------------------------------*/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        /* 1. Header / Property Handling */
        def ctx = determineContext(message, messageLog)

        /* 2. Mapping der eingehenden Payload */
        String mappedPayload = mapRequestPayload(message.getBody(String), messageLog)

        /* 3. SHA-1 Digest bilden & in Payload einfügen */
        String payloadWithDigest = addIRmark(mappedPayload, messageLog)

        /* 4. Payload im DataStore ablegen */
        persistPayload(payloadWithDigest, message, messageLog)

        /* 5. HTTP-Aufruf „Set Results“ durchführen */
        callSetResults(payloadWithDigest, ctx, messageLog)

        /* Ergebnis als neuen Body setzen */
        message.setBody(payloadWithDigest)

    } catch (Exception e) {
        handleError(message.getBody(String) as String, e, messageLog)
    }
    return message
}


/* =========================================================================
 *  Kontext-Ermittlung (Header & Properties)
 * -------------------------------------------------------------------------*/
private Map determineContext(Message message, def messageLog) {
    /*
     * Liest benötigte Properties / Header aus dem Message-Objekt.
     * Fehlen Werte, werden „placeholder“ verwendet.
     */
    def props   = message.getProperties()
    def headers = message.getHeaders()

    def ctx = [
        requestUser    : (props?.requestUser    ?: headers?.requestUser    ?: 'placeholder'),
        requestPassword: (props?.requestPassword?: headers?.requestPassword?: 'placeholder'),
        requestURL     : (props?.requestURL     ?: headers?.requestURL     ?: 'placeholder')
    ]
    messageLog?.addAttachmentAsString('ContextInfo', ctx.toString(), 'text/plain')
    return ctx
}


/* =========================================================================
 *  Mapping-Funktion
 * -------------------------------------------------------------------------*/
private String mapRequestPayload(String sourceXml, def messageLog) {
    /*
     * Erstellt den Ziel-XML-Aufbau gemäß Mapping-Spezifikation.
     * Die Felder, die nicht gemappt werden, bleiben leer.
     */
    def slurper  = new XmlSlurper(false, false)
    def root     = slurper.parseText(sourceXml)

    String senderID  = root.SenderID.text()
    String value     = root.Value.text()
    String product   = root.Product.text()
    String timestamp = root.Timestamp.text()
    String periodEnd = root.PeriodEnd.text()

    def sw = new StringWriter()
    def xml = new StreamingMarkupBuilder().bind {
        mkp.declareNamespace('' : 'http://www.govtalk.gov.uk/CM/envelope')
        GovTalkMessage {
            EnvelopeVersion('')
            Header {
                MessageDetails {
                    Class('')
                    Qualifier('')
                }
                SenderDetails {
                    IDAuthentication {
                        SenderID(senderID)
                        Authentication {
                            Method('')
                            Value(value)
                        }
                    }
                }
            }
            GovTalkDetails {
                ChannelRouting {
                    Channel {
                        Product(product)
                    }
                    Timestamp(timestamp)
                }
            }
            Body {
                mkp.declareNamespace('fps' :
                        'http://www.govtalk.gov.uk/taxation/PAYE/RTI/FullPaymentSubmission/21-22/1')
                'fps:IRenvelope'('xmlns':
                        'http://www.govtalk.gov.uk/taxation/PAYE/RTI/FullPaymentSubmission/21-22/1') {
                    IRheader {
                        Keys {
                            Key('Type' : 'TaxOfficeNumber', '')
                            Key('Type' : 'TaxOfficeReference', '')
                        }
                        PeriodEnd(periodEnd)
                        Sender('')
                        /* IRmark wird erst in addIRmark() ergänzt */
                    }
                    FullPaymentSubmission {
                        RelatedTaxYear('')
                    }
                }
            }
        }
    }
    sw << xml
    String result = sw.toString()
    messageLog?.addAttachmentAsString('MappedPayload', result, 'text/xml')
    return result
}


/* =========================================================================
 *  Digest-Funktion
 * -------------------------------------------------------------------------*/
private String addIRmark(String payload, def messageLog) {
    /*
     * 1. SHA-1 Digest über die aktuelle Payload bilden
     * 2. Element IRmark in den XML-Baum einfügen
     */
    MessageDigest md = MessageDigest.getInstance('SHA-1')
    byte[] sha1      = md.digest(payload.getBytes('UTF-8'))
    String irMark    = sha1.encodeHex().toString()

    def parser  = new XmlParser(false, false)
    def doc     = parser.parseText(payload)

    // Navigiere zu IRheader (Namespace: FPS)
    def fpsNs = new groovy.xml.Namespace(
            'http://www.govtalk.gov.uk/taxation/PAYE/RTI/FullPaymentSubmission/21-22/1', '')
    def irHeaderNode = doc.Body[fpsNs.IRenvelope].IRheader[0]

    // IRmark einfügen (gleiche FPS-Namespace)
    irHeaderNode.appendNode(
            new QName('http://www.govtalk.gov.uk/taxation/PAYE/RTI/FullPaymentSubmission/21-22/1',
                    'IRmark'), irMark)

    String finalPayload = XmlUtil.serialize(doc)
    messageLog?.addAttachmentAsString('PayloadWithIRmark', finalPayload, 'text/xml')
    return finalPayload
}


/* =========================================================================
 *  DataStore-Persistierung
 * -------------------------------------------------------------------------*/
private void persistPayload(String payload, Message message, def messageLog) {
    /*
     * Schreibt die erweiterte Payload in den DataStore „FPS21-22“.
     * Als EntryId wird – sofern vorhanden – SAP_MessageID oder CamelMessageId,
     * ansonsten ein generierter UUID-Wert verwendet.
     */
    try {
        def service = new Factory(DataStoreService.class).getService()
        if (service) {
            def entryId = message.getHeaders()?.SAP_MessageID ?:
                          message.getHeaders()?.CamelMessageId ?:
                          UUID.randomUUID().toString()

            def bean   = new DataBean()
            bean.setDataAsArray(payload.getBytes('UTF-8'))

            def cfg    = new DataConfig()
            cfg.setStoreName('FPS21-22')
            cfg.setId(entryId)
            cfg.setOverwrite(true)

            service.put(bean, cfg)
        } else {
            throw new IllegalStateException('DataStoreService konnte nicht instanziiert werden.')
        }
    } catch (Exception e) {
        handleError(payload, e, messageLog)
    }
}


/* =========================================================================
 *  HTTP-API Call „Set Results“
 * -------------------------------------------------------------------------*/
private void callSetResults(String payload, Map ctx, def messageLog) {
    /*
     * Führt den POST-Aufruf an die HMRC-Schnittstelle durch.
     * Basic-Authentication mit requestUser / requestPassword.
     */
    try {
        def url  = new URL(ctx.requestURL)
        def conn = (HttpURLConnection) url.openConnection()
        conn.setRequestMethod('POST')
        String authString = "${ctx.requestUser}:${ctx.requestPassword}"
        String authHeader = Base64.encoder.encodeToString(authString.getBytes('UTF-8'))
        conn.setRequestProperty('Authorization', "Basic ${authHeader}")
        conn.setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')
        conn.setDoOutput(true)

        conn.outputStream.withWriter('UTF-8') { it << payload }

        int rc = conn.responseCode
        messageLog?.setStringProperty('SetResultsHTTPCode', rc.toString())

        if (rc >= 400) {
            String errorStream = conn.errorStream?.text
            throw new RuntimeException("HTTP-Fehler ${rc}: ${errorStream}")
        }
    } catch (Exception e) {
        handleError(payload, e, messageLog)
    }
}


/* =========================================================================
 *  Zentrales Error-Handling
 * -------------------------------------------------------------------------*/
def handleError(String body, Exception e, def messageLog) {
    /*
     * Fügt den aktuellen Payload als Attachment hinzu und wirft eine
     * RuntimeException mit sprechender Fehlermeldung.
     */
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im FPS-Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}