/****************************************************************************************
 * Groovy-Script  : FPS_Integration.groovy
 * Beschreibung   : Erstellung der FPS Full Payment Submission inkl. IRmark,
 *                  Ablage im Datastore und Versand an HMRC-Service.
 * Autor          : ChatGPT – Senior Integration Developer
 * Version        : 1.0
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.security.MessageDigest
import com.sap.it.api.asdk.runtime.Factory
import com.sap.it.api.asdk.datastore.DataStoreService
import com.sap.it.api.asdk.datastore.DataBean
import com.sap.it.api.asdk.datastore.DataConfig

/**************************************/
/*  Main Entry Point                  */
/**************************************/
Message processData(Message message) {

    // MessageLog für Monitoring ermitteln
    def messageLog = messageLogFactory?.getMessageLog(message)

    // Ursprungs-Payload sichern (benötigt für Error Handling)
    String originalBody = message.getBody(String) as String

    try {

        /* 1. Konfiguration (Header & Properties) ermitteln               */
        def cfg = readConfiguration(message)

        /* 2. Mapping OHNE IRmark generieren                              */
        def mappingWithoutIRmark = mapRequest(originalBody, null)

        /* 3. SHA1-Digest berechnen                                       */
        String irmarkValue = createSHA1(mappingWithoutIRmark)

        /* 4. Mapping MIT IRmark generieren                               */
        def finalPayload = mapRequest(originalBody, irmarkValue)

        /* 5. Payload im Datastore sichern                                */
        storeInDatastore(finalPayload, cfg.messageId, messageLog)

        /* 6. HMRC-API aufrufen                                           */
        int httpStatus = callSetResults(cfg.requestURL, cfg.requestUser,
                                        cfg.requestPassword, finalPayload, messageLog)

        /* 7. Ergebnis in Log schreiben                                   */
        messageLog?.addAttachmentAsString("FPS-Payload", finalPayload, "text/xml")
        messageLog?.setStringProperty("HMRC-HTTP-Status", httpStatus.toString())

        /* 8. Payload an Folge-Step weitergeben                           */
        message.setBody(finalPayload)
        return message

    } catch(Exception e){
        handleError(originalBody, e, messageLog)
        return message   // wird nie erreicht – handleError wirft Exception
    }
}

/**************************************/
/*  Modul: readConfiguration          */
/**************************************/
private Map readConfiguration(Message msg){

    /*  Ermittelt Properties / Header oder setzt Platzhalter             */
    def props       = msg.getProperties() ?: [:]
    def headers     = msg.getHeaders()    ?: [:]

    String user     = props['requestUser']     ?: headers['requestUser']     ?: 'placeholder'
    String pwd      = props['requestPassword'] ?: headers['requestPassword'] ?: 'placeholder'
    String url      = props['requestURL']      ?: headers['requestURL']      ?: 'placeholder'
    String msgId    = headers['CamelMessageId'] ?: UUID.randomUUID().toString()

    return [ requestUser     : user,
             requestPassword : pwd,
             requestURL      : url,
             messageId       : msgId ]
}

/**************************************/
/*  Modul: mapRequest                 */
/**************************************/
private String mapRequest(String inputXml, String irmark){

    /*  Eingehenden Payload parsen                                       */
    def root    = new XmlSlurper().parseText(inputXml)
    def getVal  = { String tag -> root.depthFirst().find{ it.name().localPart == tag }?.text() ?: '' }

    String senderID  = getVal('SenderID')
    String authValue = getVal('Value')
    String product   = getVal('Product')
    String timestamp = getVal('Timestamp')
    String periodEnd = getVal('PeriodEnd')

    /*  Ziel-XML mittels MarkupBuilder erzeugen                          */
    StringWriter writer = new StringWriter()
    def builder = new MarkupBuilder(writer)

    builder.'GovTalkMessage'('xmlns': 'http://www.govtalk.gov.uk/CM/envelope'){

        'EnvelopeVersion'('')                      // leer gemäß Vorgabe

        Header{
            MessageDetails{
                Class('')                          // leer gemäß Vorgabe
                Qualifier('')
            }
            SenderDetails{
                IDAuthentication{
                    SenderID(senderID)
                    Authentication{
                        Method('')
                        Value(authValue)
                    }
                }
            }
        }

        GovTalkDetails{
            ChannelRouting{
                Channel{
                    Product(product)
                }
                Timestamp(timestamp)
            }
        }

        Body{
            'IRenvelope'('xmlns': 'http://www.govtalk.gov.uk/taxation/PAYE/RTI/FullPaymentSubmission/21-22/1'){
                IRheader{
                    if(irmark){ IRmark(irmark) }   // nur bei zweitem Aufruf vorhanden
                    Keys{
                        Key('TaxOfficeNumber',   [Type:'TaxOfficeNumber'])
                        Key('TaxOfficeReference',[Type:'TaxOfficeReference'])
                    }
                    PeriodEnd(periodEnd)
                    Sender('')
                }
                FullPaymentSubmission{
                    RelatedTaxYear('')
                }
            }
        }
    }
    return writer.toString()
}

/**************************************/
/*  Modul: createSHA1                 */
/**************************************/
private String createSHA1(String data){

    /*  SHA-1 Hash bilden und als HEX-String zurückgeben                 */
    byte[] bytes = data.getBytes('UTF-8')
    MessageDigest md = MessageDigest.getInstance('SHA-1')
    byte[] digest = md.digest(bytes)
    return digest.collect{ String.format('%02x', it) }.join()
}

/**************************************/
/*  Modul: storeInDatastore           */
/**************************************/
private void storeInDatastore(String payload, String entryId, def messageLog){

    try{
        def service = new Factory(DataStoreService.class).getService()
        if(service){
            DataBean dBean   = new DataBean()
            dBean.setDataAsArray(payload.getBytes('UTF-8'))

            DataConfig dConf = new DataConfig()
            dConf.setStoreName('FPS21-22')
            dConf.setId(entryId)
            dConf.setOverwrite(true)

            service.put(dBean, dConf)
            messageLog?.setStringProperty('DatastoreWrite', "ok")
        } else{
            throw new RuntimeException('DatastoreService konnte nicht initialisiert werden')
        }
    }catch(Exception ex){
        throw new RuntimeException("Fehler beim Schreiben in Datastore: ${ex.message}", ex)
    }
}

/**************************************/
/*  Modul: callSetResults (API-Call)  */
/**************************************/
private int callSetResults(String url, String user, String pwd, String payload, def messageLog){

    HttpURLConnection conn = null
    try{
        conn = (HttpURLConnection) new URL(url).openConnection()
        conn.with{
            setRequestMethod('POST')
            setDoOutput(true)
            setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
            String auth = "${user}:${pwd}".bytes.encodeBase64().toString()
            setRequestProperty('Authorization', "Basic ${auth}")
        }

        conn.outputStream.withWriter('UTF-8'){ it << payload }

        int status = conn.responseCode
        messageLog?.setStringProperty('HTTPStatus', status.toString())
        return status

    } finally{
        conn?.disconnect()
    }
}

/**************************************/
/*  Modul: handleError                */
/**************************************/
private void handleError(String body, Exception e, def messageLog){
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im FPS-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}