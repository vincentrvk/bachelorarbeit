/*************************************************************************************************
 *  Groovy-Skript: eFiling EPS Import UK (Employer Payment Summary 21-22)
 *  Autor:        Senior-Integration-Developer
 *  Beschreibung: 
 *      –  Erstellt das Ziel-XML gem. Spezifikation
 *      –  Bildet den SHA1-Digest (IRmark)
 *      –  Persistiert den eingehenden Payload im DataStore „EPS21-22“
 *      –  Sendet das Ergebnis via HTTP-POST (Basic Auth)
 *  Modularer Aufbau gem. Vorgaben
 *************************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.datastore.*          // DataStoreService, DataBean, DataConfig
import com.sap.it.api.asdk.runtime.Factory
import java.security.MessageDigest
import groovy.xml.*


/* ==============================================================================================
 *  Haupt-Einstiegspunkt
 * ============================================================================================*/
def Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)          // CPI Message-Logger

    try {
        /* 1. Properties & Header Handling -----------------------------------------------------*/
        def props = determineProperties(message, messageLog)

        /* 2. Original Payload sichern ---------------------------------------------------------*/
        writePayloadToDatastore(message, messageLog)

        /* 3. Mapping -------------------------------------------------------------------------*/
        String sourceXml  = message.getBody(String)                     // eingehender Payload
        String targetXml  = mapRequest(sourceXml, messageLog)          // Mapped XML (ohne IRmark)

        /* 4. SHA1-Digest (IRmark) -------------------------------------------------------------*/
        String targetWithDigest = addMessageDigest(targetXml, messageLog)

        /* 5. Body aktualisieren ---------------------------------------------------------------*/
        message.setBody(targetWithDigest)

        /* 6. API-Call „Set Results“ -----------------------------------------------------------*/
        callSetResults(targetWithDigest, props, messageLog)

        return message

    } catch (Exception e) {
        // Zentraler Error-Handler
        handleError(message.getBody(String) as String, e, messageLog)
    }
}


/* ==============================================================================================
 *  Funktion: determineProperties
 *  Zweck   : Liest Properties aus dem Message-Objekt oder setzt „placeholder“
 * ============================================================================================*/
def Map determineProperties(Message message, def messageLog){
    def p = [:]
    ['requestUser','requestPassword','requestURL'].each{ key ->
        def val = message.getProperty(key) ?: 'placeholder'
        p[key] = val
        messageLog?.addAttachmentAsString("Property-${key}", val, "text/plain")
    }
    return p
}


/* ==============================================================================================
 *  Funktion: writePayloadToDatastore
 *  Zweck   : Persistiert den Original-Payload im DataStore „EPS21-22“
 * ============================================================================================*/
def void writePayloadToDatastore(Message message, def messageLog){
    def dsService = new Factory(DataStoreService.class).getService()

    if(dsService){
        String msgId = (message.getHeaders()['SAP_MessageProcessingId'] ?: 
                        message.getHeaders()['MessageId']              ?: 
                        UUID.randomUUID().toString())

        DataBean   dBean   = new DataBean()
        dBean.setDataAsArray(message.getBody(String).getBytes("UTF-8"))

        DataConfig dConfig = new DataConfig()
        dConfig.setStoreName("EPS21-22")
        dConfig.setId(msgId)
        dConfig.setOverwrite(true)

        dsService.put(dBean, dConfig)
        messageLog?.addAttachmentAsString("Datastore-Info",
                "Payload unter EntryId '${msgId}' in DataStore 'EPS21-22' abgelegt.","text/plain")
    }else{
        messageLog?.addAttachmentAsString("Datastore-Warnung",
                "DataStoreService nicht verfügbar – Payload wurde nicht persistiert.","text/plain")
    }
}


/* ==============================================================================================
 *  Funktion: mapRequest
 *  Zweck   : Erstellt das Ziel-XML gemäß Mapping-Vorgabe (noch ohne IRmark)
 * ============================================================================================*/
def String mapRequest(String sourceXml, def messageLog){

    // Eingehendes XML parsen (Namespace im Input ist egal für unsere Felder)
    def src = new XmlSlurper().parseText(sourceXml)

    final String envNs = 'http://www.govtalk.gov.uk/CM/envelope'
    final String rtiNs = 'http://www.govtalk.gov.uk/taxation/PAYE/RTI/EmployerPaymentSummary/21-22/1'

    // StreamingMarkupBuilder generiert performantes XML
    def smb = new StreamingMarkupBuilder(encoding:'UTF-8')
    def target = smb.bind {

        mkp.xmlDeclaration(version:"1.0", encoding:"UTF-8")
        mkp.declareNamespace('':envNs, 'rt':rtiNs)

        'GovTalkMessage' {

            'EnvelopeVersion'('')

            'Header'{
                'MessageDetails'{
                    'Class'('')
                    'Qualifier'('')
                }
                'SenderDetails'{
                    'IDAuthentication'{
                        'SenderID'(src.SenderID.text())
                        'Authentication'{
                            'Method'('')
                            'Value'(src.Value.text())
                        }
                    }
                }
            }

            'GovTalkDetails'{
                'ChannelRouting'{
                    'Channel'{
                        'Product'(src.Product.text())
                    }
                    'Timestamp'(src.Timestamp.text())
                }
            }

            'Body'{
                'rt:IRenvelope'('xmlns':rtiNs){
                    'rt:IRheader'{
                        'rt:Keys'{
                            'rt:Key'('Type':'TaxOfficeNumber')
                            'rt:Key'('Type':'TaxOfficeReference')
                        }
                        'rt:PeriodEnd'(src.PeriodEnd.text())
                        'rt:Sender'('')
                        'rt:IRmark'('')                                   // Platzhalter – wird später gefüllt
                    }
                    'rt:EmployerPaymentSummary'{
                        'rt:RelatedTaxYear'('')
                    }
                }
            }
        }
    }

    String xmlString = target.toString()
    messageLog?.addAttachmentAsString("Mapped-XML (ohne IRmark)", xmlString, "text/xml")
    return xmlString
}


/* ==============================================================================================
 *  Funktion: addMessageDigest
 *  Zweck   : Berechnet SHA1 auf dem bereitgestellten XML (ohne IRmark) 
 *            und schreibt den Wert in das IRmark-Element
 * ============================================================================================*/
def String addMessageDigest(String xmlWithoutDigest, def messageLog){

    /* SHA1-Digest berechnen -----------------------------------------------------------*/
    MessageDigest md = MessageDigest.getInstance("SHA-1")
    byte[] digestBytes = md.digest(xmlWithoutDigest.getBytes("UTF-8"))
    String sha1Hex     = digestBytes.collect{ String.format("%02x", it) }.join()

    /* IRmark setzen ------------------------------------------------------------------*/
    final String rtiNs = 'http://www.govtalk.gov.uk/taxation/PAYE/RTI/EmployerPaymentSummary/21-22/1'

    def parser = new XmlParser(false, true)         // namespace-aware Parser
    Node root = parser.parseText(xmlWithoutDigest)

    // IRmark-Node suchen & befüllen
    Node irMarkNode = root.depthFirst().find{ Node n ->
        n.name() instanceof javax.xml.namespace.QName && 
        n.name().localPart == 'IRmark'
    } as Node

    if(irMarkNode){
        irMarkNode.value = sha1Hex
    }

    String finalXml = XmlUtil.serialize(root)
    messageLog?.addAttachmentAsString("Final-XML (inkl. IRmark)", finalXml, "text/xml")
    return finalXml
}


/* ==============================================================================================
 *  Funktion: callSetResults
 *  Zweck   : HTTP-POST (Basic) an die Property-URL – ohne Response-Weiterverarbeitung
 * ============================================================================================*/
def void callSetResults(String payload, Map props, def messageLog){

    if(props.requestURL == 'placeholder'){
        messageLog?.addAttachmentAsString("HTTP-Warnung",
              "requestURL = placeholder – HTTP-Call übersprungen.","text/plain")
        return
    }

    URL url = new URL(props.requestURL)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()

    String authRaw  = "${props.requestUser}:${props.requestPassword}"
    String authEnc  = authRaw.bytes.encodeBase64().toString()

    conn.setRequestMethod("POST")
    conn.setRequestProperty("Authorization", "Basic ${authEnc}")
    conn.setDoOutput(true)

    conn.outputStream.withWriter("UTF-8"){ writer ->
        writer << payload
    }

    int rc = conn.responseCode
    messageLog?.addAttachmentAsString("HTTP-Status",
            "Set Results – ResponseCode: ${rc}", "text/plain")
}


/* ==============================================================================================
 *  Funktion: handleError  (Vorlage aus Aufgabenstellung)
 * ============================================================================================*/
def handleError(String body, Exception e, def messageLog) {
    // Logging im Monitoring (name, inhalt, typ)
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im EPS-Integration-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}