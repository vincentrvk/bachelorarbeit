/****************************************************************************************
 *  eFiling Employees Payments UK – Earlier Year Update (EYU) 2017/18
 *  Groovy-Skript für SAP Cloud Integration
 *
 *  Aufgabenübersicht (Kurz):
 *    1.   Header/Property-Übernahme mit Fallback auf „placeholder“
 *    2.   Request-Mapping (INPUT  ➜  OUTPUT)
 *    3.   SHA-1-Digest bilden und als <IRmark> einfügen
 *    4.   Persistenz in DataStore   (Name: EYU17-18,  EntryId: CamelMessageId)
 *    5.   HTTP-POST „Set Results“  (Basic-Auth)
 *
 *  Autor:  (Senior Integration Developer)
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import groovy.xml.XmlParser
import groovy.xml.XmlUtil
import java.security.MessageDigest
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory

// Haupteinstiegspunkt für das Skript
def Message processData(Message message) {

    // MessageLog initialisieren (kann null sein, z. B. in Tests)
    def msgLog = messageLogFactory?.getMessageLog(message)
    try {
        /* 1) Header / Properties vorbereiten */
        def hp = determineHeadersAndProperties(message)

        /* 2) Mapping durchführen (noch ohne <IRmark>) */
        String mappedXml = transformPayload(message.getBody(String) ?: '')

        /* 3) SHA-1 Digest erzeugen und einfügen */
        String xmlWithDigest = insertMessageDigest(mappedXml)

        /* 4) In DataStore schreiben                                                   */
        writeToDatastore(xmlWithDigest,
                         message.getHeader('CamelMessageId', String) ?: UUID.randomUUID().toString(),
                         msgLog)

        /* 5) HTTP-Call „Set Results“ ausführen                                        */
        callSetResults(xmlWithDigest, hp.requestURL, hp.requestUser, hp.requestPassword, msgLog)

        /*  Payload im Message-Objekt aktualisieren und Rückgabe                      */
        message.setBody(xmlWithDigest)
        return message

    } catch (Exception e) {
        handleError(message.getBody(String) ?: '', e, msgLog)
        /* handleError wirft immer RuntimeException – folgender return ist rein
           formal notwendig, wird aber nie erreicht                                    */
        return message
    }
}

// -------------------------------------------------------------------------------------
//  Modul-Funktionen
// -------------------------------------------------------------------------------------

/*  Header / Property-Ermittlung                                                      */
private Map determineHeadersAndProperties(Message msg) {
    Map result = [:]

    result.requestUser     = (msg.getProperty('requestUser')     ?: msg.getHeader('requestUser',     String)) ?: 'placeholder'
    result.requestPassword = (msg.getProperty('requestPassword') ?: msg.getHeader('requestPassword', String)) ?: 'placeholder'
    result.requestURL      = (msg.getProperty('requestURL')      ?: msg.getHeader('requestURL',      String)) ?: 'placeholder'

    return result
}

/*  Mapping‐Funktion                                                                  */
private String transformPayload(String body) {
    try {
        // Namespace der Eingangsmessage (Default-Namespace)
        def envNS = new groovy.xml.Namespace('http://www.govtalk.gov.uk/CM/envelope', '')
        def src   = new XmlParser().parseText(body)

        String senderID  = src."${envNS}SenderID".text()
        String value     = src."${envNS}Value".text()
        String product   = src."${envNS}Product".text()
        String timestamp = src."${envNS}Timestamp".text()
        String periodEnd = src."${envNS}PeriodEnd".text()

        /*  Ziel-XML erzeugen mit MarkupBuilder                                        */
        StringWriter sw = new StringWriter()
        MarkupBuilder mb = new MarkupBuilder(sw)
        mb.mkp.xmlDeclaration(version: '1.0', encoding: 'UTF-8')
        mb.GovTalkMessage('xmlns': envNS.uri) {
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
                        SenderID(senderID)
                        Authentication {
                            Method()
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
                IRenvelope('xmlns':'http://www.govtalk.gov.uk/taxation/PAYE/RTI/EarlierYearUpdate/16-17/1') {
                    IRheader {
                        Keys {
                            Key(Type:'TaxOfficeNumber')
                            Key(Type:'TaxOfficeReference')
                        }
                        PeriodEnd(periodEnd)
                        Sender()
                        // <IRmark> wird später eingefügt
                    }
                    EarlierYearUpdate {
                        RelatedTaxYear()
                    }
                }
            }
        }
        return sw.toString()
    } catch(Exception e){
        throw new RuntimeException("Fehler bei transformPayload: ${e.message}", e)
    }
}

/*  Digest erzeugen und in XML einfügen                                               */
private String insertMessageDigest(String xmlWithoutDigest) {
    // SHA-1 als Hex berechnen
    MessageDigest md = MessageDigest.getInstance('SHA-1')
    String digestHex = md.digest(xmlWithoutDigest.getBytes('UTF-8')).encodeHex().toString()

    // XML parsen (namespace-aware) und <IRmark> ergänzen
    def parser = new XmlParser()
    def root   = parser.parseText(xmlWithoutDigest)

    // Suche nach IRheader (Namespace spielt hier keine Rolle, Zugriff via localPart)
    Node irHeader = root.depthFirst().find { Node n ->
        n.name() instanceof QName ? n.name().localPart == 'IRheader' : n.name() == 'IRheader'
    } as Node

    if(irHeader == null){
        throw new IllegalStateException('IRheader Element nicht gefunden – Digest kann nicht eingefügt werden.')
    }
    irHeader.appendNode('IRmark', digestHex)

    return XmlUtil.serialize(root).trim()
}

/*  DataStore-Schreibfunktion                                                         */
private void writeToDatastore(String payload, String entryId, def msgLog){
    try {
        def service = new Factory(DataStoreService.class).getService()
        if(service == null){
            msgLog?.addAttachmentAsString('DatastoreInfo','DataStoreService nicht verfügbar – Payload wird nicht persistiert.','text/plain')
            return
        }
        DataBean dBean = new DataBean()
        dBean.setDataAsArray(payload.getBytes('UTF-8'))
        DataConfig cfg = new DataConfig()
        cfg.setStoreName('EYU17-18')
        cfg.setId(entryId)
        cfg.setOverwrite(true)
        service.put(dBean, cfg)
    } catch(Exception e){
        // Fehler hier soll die Verarbeitung nicht stoppen – lediglich loggen
        msgLog?.addAttachmentAsString('DatastoreError', "Persistierung fehlgeschlagen: ${e.message}", 'text/plain')
    }
}

/*  HTTP-POST-Aufruf „Set Results“                                                    */
private void callSetResults(String xmlPayload, String url, String user, String pwd, def msgLog){
    HttpURLConnection conn = null
    try{
        conn = (HttpURLConnection) new URL(url).openConnection()
        conn.setRequestMethod('POST')
        conn.setDoOutput(true)
        String basicAuth = "${user}:${pwd}".bytes.encodeBase64().toString()
        conn.setRequestProperty('Authorization', "Basic ${basicAuth}")
        conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')

        conn.outputStream.withWriter('UTF-8'){ it << xmlPayload }

        int rc = conn.responseCode
        msgLog?.setStringProperty('HTTP-Status-SetResults', rc.toString())
    } catch(Exception e){
        // API-Fehler werden geloggt, aber Ablauf nicht gestoppt
        msgLog?.addAttachmentAsString('HTTP-Error', "SetResults-Call fehlgeschlagen: ${e.message}", 'text/plain')
    } finally{
        conn?.disconnect()
    }
}

/*  Zentrales Error-Handling                                                          */
private void handleError(String body, Exception e, def messageLog){
    // Message-Body als Attachment anhängen
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String err = "Fehler im EYU-Mapping-Skript: ${e.message}"
    throw new RuntimeException(err, e)
}