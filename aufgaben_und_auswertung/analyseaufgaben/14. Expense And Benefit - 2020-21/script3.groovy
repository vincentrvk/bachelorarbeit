/**************************************************************************
 *  Groovy-Skript für SAP Cloud Integration                               
 *  eFiling of Employees Payments – UK  (Expenses & Benefits 20/21)       
 *                                                                        
 *  Autor:  Senior-Developer (Integration & Groovy)                       
 *                                                                        
 *  Aufgaben                                                               
 *  1.   Header-/Property-Handling                                         
 *  2.   Mapping des Eingangs-Payloads                                     
 *  3.   SHA-1-Digest (IRmark) erzeugen und einfügen                       
 *  4.   Payload im DataStore sichern                                      
 *  5.   Ergebnis via HTTP-POST an HMRC schicken                           
 *                                                                        
 *  Hinweise:                                                              
 *  –  Jede Funktion ist eigenständig & enthält Fehlerbehandlung.          
 *  –  Fehlende Header/Properties werden mit „placeholder“ bestückt.       
 *  –  Bei Fehlern wird der eingehende Payload als Attachment angehängt.   
 **************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import java.security.MessageDigest
import groovy.xml.StreamingMarkupBuilder
import groovy.xml.XmlUtil
import com.sap.it.api.asdk.runtime.Factory
import com.sap.it.api.asdk.datastore.*

/* === GLOBALER EINSTIEG ================================================= */
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {

        /* 1. Header / Property lesen  */
        def cfg = getConfigParams(message, messageLog)

        /* 2. Mapping durchführen      */
        String inputPayload = message.getBody(String) ?: ''
        String mappedXml    = mapRequest(inputPayload, messageLog)

        /* 3. SHA-1 Digest bilden      */
        String finalXml     = addMessageDigest(mappedXml, messageLog)

        /* 4. In DataStore sichern     */
        writeToDataStore(finalXml, message, messageLog)

        /* 5. HTTP-POST an HMRC        */
        int httpRC          = postResults(cfg.requestURL,
                                          cfg.requestUser,
                                          cfg.requestPassword,
                                          finalXml,
                                          messageLog)

        /* Ergebnis in Message schreiben */
        message.setBody(finalXml)
        message.setHeader('HTTP_RESPONSE_CODE', httpRC)

        return message

    } catch (Exception e) {
        handleError(message.getBody(String) ?: '', e, messageLog)
    }
}

/* ======================================================================= */
/* Funktion: Header & Properties verarbeiten                               */
/* ======================================================================= */
def Map getConfigParams(Message msg, def msgLog) {
    try {
        def getVal = { String key ->
            msg.getProperty(key) ?: msg.getHeader(key, String) ?: 'placeholder'
        }
        return [
            requestUser    : getVal('requestUser'),
            requestPassword: getVal('requestPassword'),
            requestURL     : getVal('requestURL')
        ]
    } catch (Exception e) {
        handleError(msg.getBody(String) ?: '', e, msgLog)
    }
}

/* ======================================================================= */
/* Funktion: Eingangs-XML -> Ziel-XML mappen                               */
/* ======================================================================= */
def String mapRequest(String sourceXml, def msgLog) {

    try {
        /* Eingangs-Payload parsen (Namespace ist bereits Root-NS) */
        def inSlurper = new XmlSlurper().parseText(sourceXml)
        inSlurper.declareNamespace(envelope: 'http://www.govtalk.gov.uk/CM/envelope')

        /* Felder holen */
        String senderID       = inSlurper.SenderID.text()
        String authValue      = inSlurper.Value.text()
        String product        = inSlurper.Product.text()
        String timestamp      = inSlurper.Timestamp.text()
        String defaultCurrency= inSlurper.DefaultCurrency.text()

        /* Ziel-XML mit StreamingMarkupBuilder erzeugen               */
        def builder = new StreamingMarkupBuilder(encoding: 'UTF-8')
        def target  = builder.bind {

            mkp.xmlDeclaration()
            /* Root-Namespace GovTalk */
            mkp.declareNamespace('': 'http://www.govtalk.gov.uk/CM/envelope')

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
                                Value(authValue)
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
                    /* Spezifisches Namespace für IRenvelope             */
                    IRenvelope(xmlns: 'http://www.govtalk.gov.uk/taxation/EXB/20-21/1') {
                        IRheader {
                            Keys {
                                Key('', [Type: 'TaxOfficeNumber'])
                                Key('', [Type: 'TaxOfficeReference'])
                            }
                            PeriodEnd('')
                            DefaultCurrency(defaultCurrency)
                            Sender('')
                            /* IRmark wird in späterer Funktion ergänzt    */
                        }
                        ExpensesAndBenefits('')
                    }
                }
            }
        }

        return XmlUtil.serialize(target).trim()

    } catch (Exception e) {
        handleError(sourceXml, e, msgLog)
    }
}

/* ======================================================================= */
/* Funktion: SHA-1 Digest (IRmark) einfügen                                */
/* ======================================================================= */
def String addMessageDigest(String xmlString, def msgLog) {

    try {
        /* SHA-1 Digest über das komplette Dokument OHNE IRmark bilden */
        String sha1 = computeSHA1(xmlString.getBytes('UTF-8'))

        /* Dokument parsen, IRmark anfügen                              */
        def parser   = new XmlParser(false, false)
        def rootNode = parser.parseText(xmlString)

        /* Navigation: Body -> IRenvelope -> IRheader                   */
        def body       = rootNode.'Body'[0]
        def irEnvelope = body.'IRenvelope'[0]
        def irHeader   = irEnvelope.'IRheader'[0]

        irHeader.appendNode('IRmark', sha1)

        /* Serialisieren und XML-Deklaration manuell voranstellen       */
        StringWriter sw = new StringWriter()
        new XmlNodePrinter(new PrintWriter(sw)).print(rootNode)
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + sw.toString()

    } catch (Exception e) {
        handleError(xmlString, e, msgLog)
    }
}

/* ======================================================================= */
/* Funktion: SHA-1 berechnen (Hex-String)                                  */
/* ======================================================================= */
def String computeSHA1(byte[] data) {
    MessageDigest.getInstance('SHA-1')
                 .digest(data)
                 .encodeHex()
                 .toString()
}

/* ======================================================================= */
/* Funktion: Payload in DataStore schreiben                                */
/* ======================================================================= */
def void writeToDataStore(String payload, Message msg, def msgLog) {

    try {
        def dsService = new Factory(DataStoreService.class).getService()
        if (dsService) {
            DataBean   bean   = new DataBean()
            bean.setDataAsArray(payload.getBytes('UTF-8'))

            DataConfig cfg = new DataConfig()
            cfg.setStoreName('EXB20-21')
            cfg.setId(msg.exchange?.getExchangeId() ?: UUID.randomUUID().toString())
            cfg.setOverwrite(true)

            dsService.put(bean, cfg)
        } else {
            msgLog?.addAttachmentAsString('DataStoreWarning',
                    'DataStoreService nicht verfügbar – Payload wurde nicht gesichert',
                    'text/plain')
        }
    } catch (Exception e) {
        handleError(payload, e, msgLog)
    }
}

/* ======================================================================= */
/* Funktion: HTTP-POST (Basic Auth)                                        */
/* ======================================================================= */
def int postResults(String url, String user, String pwd,
                    String payload, def msgLog) {

    try {
        def conn = new URL(url).openConnection()
        conn.requestMethod = 'POST'
        conn.doOutput      = true
        conn.setRequestProperty('Authorization',
                'Basic ' + "${user}:${pwd}".bytes.encodeBase64().toString())
        conn.setRequestProperty('Content-Type', 'application/xml')

        conn.outputStream.withWriter('UTF-8') { it << payload }
        int rc = conn.responseCode

        msgLog?.addAttachmentAsString('HTTP_Response_Code',
                rc.toString(), 'text/plain')
        return rc

    } catch (Exception e) {
        handleError(payload, e, msgLog)
    }
}

/* ======================================================================= */
/* Funktion: Zentrales Error-Handling                                      */
/* ======================================================================= */
def handleError(String body, Exception e, def messageLog) {
    /* Payload im MPL anhängen, damit dieser bei Fehlern sichtbar ist     */
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}