/*******************************************************************************
*  Groovy-Skript : FPS 21-22 – Full Payment Submission (UK)
*  Autor        : AI-Assistant
*  Beschreibung : 
*   1.  Setzt benötigte Properties/Headers auf Default-Werte („placeholder“)
*   2.  Erstellt die Request-Payload (Mapping)
*   3.  Erzeugt SHA-1-Digest und fügt diesen als <IRmark> ein
*   4.  Persistiert die fertige Payload im DataStore  „FPS21-22“
*   5.  Versendet die Payload per HTTP-POST an die in den Properties
*       definierte URL (Basic-Auth)
*******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import groovy.xml.XmlParser
import groovy.xml.XmlNodePrinter
import groovy.xml.XmlUtil
import com.sap.it.api.asdk.runtime.Factory
import com.sap.it.api.asdk.datastore.*

/* ----------------------------------------------------------- */
/*  Haupt-Einstiegspunkt                                       */
/* ----------------------------------------------------------- */
def Message processData(Message message) {
    // Logging-Instanz
    def messageLog = messageLogFactory.getMessageLog(message)
    
    try {
        /* 1) Properties & Header vorbereiten */
        setPropertiesAndHeaders(message, messageLog)
        
        /* 2) Eingehenden Payload lesen und mappen */
        String inPayload  = message.getBody(String) as String
        String reqPayload = buildRequestPayload(inPayload, messageLog)
        
        /* 3) SHA-1 Digest erzeugen und in Payload einfügen */
        String payloadWithDigest = addSHA1Digest(reqPayload, messageLog)
        message.setBody(payloadWithDigest)
        
        /* 4) Payload im DataStore sichern */
        writeToDataStore(message, payloadWithDigest.getBytes('UTF-8'), messageLog)
        
        /* 5) HTTP-POST an Zielsystem */
        sendResults(message, payloadWithDigest, messageLog)
        
        return message
    } catch(Exception e) {
        // Ursprünglichen Payload für Monitoring sichern
        String currentBody = message.getBody(String) as String
        handleError(currentBody, e, messageLog)
    }
}

/* ----------------------------------------------------------- */
/*  Funktion : setPropertiesAndHeaders                         */
/* ----------------------------------------------------------- */
/*  Liest vorhandene Properties/Headers aus dem Message-Objekt. 
*  Falls Werte fehlen, werden sie mit „placeholder“ vorbelegt.  */
def void setPropertiesAndHeaders(Message message, def messageLog){
    // Hilfsfunktion, um leere oder nicht vorhandene Werte zu prüfen
    def resolve = { val -> (val == null || (val instanceof String && val.trim().isEmpty())) ? 'placeholder' : val }
    
    String user = resolve(message.getProperty('requestUser'))
    String pwd  = resolve(message.getProperty('requestPassword'))
    String url  = resolve(message.getProperty('requestURL'))
    
    message.setProperty('requestUser', user)
    message.setProperty('requestPassword', pwd)
    message.setProperty('requestURL', url)
    
    // Für evtl. benötigte Weiterverarbeitung auch als Header ablegen
    message.setHeader('requestUser', user)
    message.setHeader('requestPassword', pwd)
    message.setHeader('requestURL', url)
    
    messageLog?.addAttachmentAsString('Config', "URL=${url}\nUser=${user}", 'text/plain')
}

/* ----------------------------------------------------------- */
/*  Funktion : buildRequestPayload                             */
/* ----------------------------------------------------------- */
/*  Wandelt den eingehenden XML-Payload in die geforderte       *
*  GovTalk-Struktur um.                                        */
def String buildRequestPayload(String inputXml, def messageLog){
    def source = new XmlSlurper().parseText(inputXml)
    
    // Writer & Builder für Zieldokument
    StringWriter writer = new StringWriter()
    MarkupBuilder xml = new MarkupBuilder(writer)
    
    xml.'GovTalkMessage'('xmlns':'http://www.govtalk.gov.uk/CM/envelope') {
        'EnvelopeVersion'('')
        'Header' {
            'MessageDetails' {
                'Class'     ('')
                'Qualifier' ('')
            }
            'SenderDetails' {
                'IDAuthentication' {
                    'SenderID'     (source.SenderID.text())
                    'Authentication' {
                        'Method' ('')
                        'Value'  (source.Value.text())
                    }
                }
            }
        }
        'GovTalkDetails' {
            'ChannelRouting' {
                'Channel' {
                    'Product' (source.Product.text())
                }
                'Timestamp' (source.Timestamp.text())
            }
        }
        'Body' {
            'IRenvelope'('xmlns':'http://www.govtalk.gov.uk/taxation/PAYE/RTI/FullPaymentSubmission/21-22/1') {
                'IRheader' {
                    'Keys' {
                        'Key'('' , ['Type':'TaxOfficeNumber'])
                        'Key'('' , ['Type':'TaxOfficeReference'])
                    }
                    'PeriodEnd' (source.PeriodEnd.text())
                    'Sender'    ('')
                    // <IRmark> wird erst nach Digest-Berechnung eingefügt
                }
                'FullPaymentSubmission' {
                    'RelatedTaxYear' ('')
                }
            }
        }
    }
    
    String result = writer.toString()
    messageLog?.addAttachmentAsString('MappedPayload', result, 'text/xml')
    return result
}

/* ----------------------------------------------------------- */
/*  Funktion : addSHA1Digest                                   */
/* ----------------------------------------------------------- */
/*  Bildet den SHA-1 Digest der übergebenen Payload, fügt einen *
*  <IRmark> Knoten in die IRheader-Struktur ein und liefert die*
*  finale Payload zurück.                                      */
def String addSHA1Digest(String xmlPayload, def messageLog){
    // Digest berechnen
    def md = java.security.MessageDigest.getInstance('SHA-1')
    byte[] digestBytes = md.digest(xmlPayload.getBytes('UTF-8'))
    String digest      = digestBytes.encodeHex().toString()
    
    // XML modifizieren – IRmark hinzufügen
    XmlParser parser = new XmlParser(false, false)
    Node root = parser.parseText(xmlPayload)
    // Navigieren zu IRheader
    Node irHeader = root.Body[0].IRenvelope[0].IRheader[0]
    irHeader.children().add(0, new Node(irHeader, 'IRmark', digest))
    
    // Serialisieren
    StringWriter sw = new StringWriter()
    new XmlNodePrinter(new PrintWriter(sw)).print(root)
    String finalXml = sw.toString()
    
    messageLog?.addAttachmentAsString('PayloadWithDigest', finalXml, 'text/xml')
    return finalXml
}

/* ----------------------------------------------------------- */
/*  Funktion : writeToDataStore                                */
/* ----------------------------------------------------------- */
/*  Persistiert die Payload im DataStore „FPS21-22“ unter der   *
*  EntryId = CamelMessageId der aktuellen Message.             */
def void writeToDataStore(Message message, byte[] data, def messageLog){
    def dsService = new Factory(DataStoreService.class).getService()
    
    if(dsService == null){
        messageLog?.addAttachmentAsString('DatastoreError', 'Kein DataStoreService verfügbar', 'text/plain')
        return
    }
    
    String entryId = message.getHeader('CamelMessageId', String.class) ?: UUID.randomUUID().toString()
    
    DataBean   dBean   = new DataBean()
    dBean.setDataAsArray(data)
    
    DataConfig dConfig = new DataConfig()
    dConfig.setStoreName('FPS21-22')
    dConfig.setId(entryId)
    dConfig.setOverwrite(true)
    
    dsService.put(dBean, dConfig)
    messageLog?.addAttachmentAsString('DatastoreInfo', "EntryId=${entryId}", 'text/plain')
}

/* ----------------------------------------------------------- */
/*  Funktion : sendResults                                     */
/* ----------------------------------------------------------- */
/*  Führt einen HTTP-POST gegen die per Property angegebenen    *
*  URL aus (Basic-Authentication).                             */
def void sendResults(Message message, String payload, def messageLog){
    String urlStr = message.getProperty('requestURL')
    String user   = message.getProperty('requestUser')
    String pwd    = message.getProperty('requestPassword')
    
    URL connUrl = new URL(urlStr)
    HttpURLConnection conn = (HttpURLConnection) connUrl.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Content-Type', 'application/xml')
    
    // Basic-Auth Header setzen
    String auth = "${user}:${pwd}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${auth}")
    
    // Payload schreiben
    conn.outputStream.withWriter('UTF-8') { it << payload }
    
    int responseCode = conn.responseCode
    message.setHeader('HTTP_RESPONSE_CODE', responseCode)
    messageLog?.addAttachmentAsString('HTTP_RESPONSE', "Code=${responseCode}", 'text/plain')
}

/* ----------------------------------------------------------- */
/*  Funktion : handleError                                     */
/* ----------------------------------------------------------- */
/*  Globales Error-Handling – schreibt den fehlerhaften Payload *
*  als Attachment in das Monitoring und wirft RuntimeException.*/
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}