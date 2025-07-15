/***************************************************************
*  eFiling UK – Earlier Year Update (EYU) 17-18
*  ------------------------------------------------------------
*  1.  Mapping des Eingangs-Payloads
*  2.  SHA-1-Digest bilden und in XML einfügen
*  3.  Persistenz in Datastore „EYU17-18“
*  4.  HTTP-POST (Basic-Auth) an HMRC-Endpunkt
*  5.  Zentrales Error-Handling inkl. Payload-Attachment
****************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.runtime.Factory
import com.sap.it.api.asdk.datastore.DataStoreService
import com.sap.it.api.asdk.datastore.DataBean
import com.sap.it.api.asdk.datastore.DataConfig
import java.security.MessageDigest
import groovy.xml.MarkupBuilder
import java.net.HttpURLConnection
import java.net.URL

// =======================  MAIN  ===============================
Message processData(Message message) {

    // Ursprungs-Payload sichern (für Logging & Error-Handling)
    String originalBody = message.getBody(String) as String
    def messageLog     = messageLogFactory?.getMessageLog(message)

    try {
        /* 1. Properties & Header auslesen */
        def creds        = resolvePropertiesAndHeaders(message)

        /* 2. Eingangs-XML parsen & mappen  (Variante ohne Digest) */
        Map<String,String> inputMap          = parseInput(originalBody)
        String xmlWithoutDigest              = buildRequestXml(inputMap, null)

        /* 3. SHA-1 Digest berechnen & Payload finalisieren */
        String digest                         = computeSHA1(xmlWithoutDigest)
        String finalPayload                   = buildRequestXml(inputMap, digest)

        /* 4. Datastore-Persistenz */
        writeToDataStore(message, finalPayload)

        /* 5. HTTP-Aufruf */
        performPost(creds.url, creds.user, creds.pass, finalPayload, messageLog)

        /* 6. Body an nachfolgende Schritte übergeben */
        message.setBody(finalPayload)
        return message

    } catch (Exception e) {
        handleError(originalBody, e, messageLog)
    }
}
// ===================  HELPER FUNCTIONS  ======================

/* Properties & Header ermitteln – Fallback auf „placeholder“ */
private Map<String,String> resolvePropertiesAndHeaders(Message msg) {
    String val(String key){
        msg.getProperty(key) ?: msg.getHeader(key, String) ?: 'placeholder'
    }
    [user: val('requestUser'),
     pass: val('requestPassword'),
     url : val('requestURL')]
}

/* Eingangs-XML → Map */
private Map<String,String> parseInput(String xmlString) {
    def root = new XmlSlurper().parseText(xmlString)
    [SenderID : root.SenderID.text(),
     Value    : root.Value.text(),
     Product  : root.Product.text(),
     Timestamp: root.Timestamp.text(),
     PeriodEnd: root.PeriodEnd.text()]
}

/* Ziel-XML aufbauen – optional mit IRmark */
private String buildRequestXml(Map m, String digest) {
    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw);  xml.setDoubleQuotes(true)
    xml.GovTalkMessage(xmlns:'http://www.govtalk.gov.uk/CM/envelope'){
        EnvelopeVersion()
        Header{
            MessageDetails{
                Class(); Qualifier(); CorrelationID(); GatewayTimestamp()
            }
            SenderDetails{
                IDAuthentication{
                    SenderID(m.SenderID)
                    Authentication{ Method(); Value(m.Value) }
                }
            }
        }
        GovTalkDetails{
            ChannelRouting{
                Channel{ Product(m.Product) }
                Timestamp(m.Timestamp)
            }
        }
        Body{
            'IRenvelope'('xmlns':'http://www.govtalk.gov.uk/taxation/PAYE/RTI/EarlierYearUpdate/16-17/1'){
                IRheader{
                    Keys{ Key(Type:'TaxOfficeNumber'); Key(Type:'TaxOfficeReference') }
                    PeriodEnd(m.PeriodEnd)
                    Sender()
                    if(digest) { IRmark(digest) }
                }
                EarlierYearUpdate{ RelatedTaxYear() }
            }
        }
    }
    sw.toString()
}

/* SHA-1-Digest als Hex-String */
private String computeSHA1(String txt){
    MessageDigest md = MessageDigest.getInstance('SHA-1')
    md.digest(txt.getBytes('UTF-8')).encodeHex().toString()
}

/* Persistenz in Datastore „EYU17-18“ */
private void writeToDataStore(Message msg, String payload){
    DataStoreService svc = new Factory(DataStoreService.class).getService()
    if(!svc) throw new IllegalStateException('DataStoreService nicht verfügbar.')
    String entryId = msg.getProperty('SAP_MessageProcessingId') ?: UUID.randomUUID().toString()
    DataBean    bean = new DataBean()
    bean.setDataAsArray(payload.getBytes('UTF-8'))
    DataConfig  cfg  = new DataConfig()
    cfg.setStoreName('EYU17-18'); cfg.setId(entryId); cfg.setOverwrite(true)
    svc.put(bean, cfg)
}

/* HTTP-POST mit Basic-Auth  */
private void performPost(String url, String user, String pass, String payload, def log){
    if(!url || 'placeholder' == url){ 
        log?.addAttachmentAsString('HTTP-Skip',"URL 'placeholder' – Aufruf übersprungen","text/plain")
        return
    }
    HttpURLConnection con = (HttpURLConnection)new URL(url).openConnection()
    con.requestMethod = 'POST'; con.doOutput = true
    con.setRequestProperty('Authorization','Basic '+("${user}:${pass}".bytes.encodeBase64()))
    con.setRequestProperty('Content-Type','application/xml; charset=UTF-8')
    con.outputStream.withWriter('UTF-8'){ it<<payload }
    log?.addAttachmentAsString('HTTP-ResponseCode', con.responseCode.toString(),'text/plain')
}

/* Zentrales Error-Handling inkl. Attachment */
private void handleError(String body, Exception e, def messageLog){
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    throw new RuntimeException("Fehler im Mapping-Skript: ${e.message}", e)
}