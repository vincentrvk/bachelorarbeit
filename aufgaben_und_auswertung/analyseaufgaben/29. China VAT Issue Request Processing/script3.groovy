/****************************************************************************************
 *  SAP Document & Reporting Compliance – VAT Issue China
 *  ----------------------------------------------------------------------------
 *  Groovy-Skript für SAP Cloud Integration
 *  
 *  Dieses Skript übernimmt folgende Aufgaben:
 *    1.   Lesen von bis zu 10 Einträgen aus dem DataStore "IssueRequest_Storage"
 *    2.   Für jeden Eintrag: Extraktion von TaxNumber, Base64-Kodierung, optionales
 *         Signieren des Payloads, Aufruf der chinesischen Steuerbehörde (HTTP POST)
 *    3.   Verarbeitung der Response:
 *           • Ablegen eines Status-Dokuments pro GTHeader im DataStore 
 *             "GoldenTaxDocument_Result"
 *           • Mapping des Response – Ablegen im DataStore "QueryRequest_Storage"
 *    4.   Fehlerbehandlung inkl. Attachment des fehlerhaften Payloads
 *
 *  Autor:  Senior-Integration-Developer
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets

//==== 1. Einstiegspunkt ================================================================
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {

        // Initiale Konfiguration laden oder mit Platzhaltern befüllen
        def cfg = initConfig(message)

        // DataStore-Service für globale DataStores holen
        def dsService = (DataStoreService) new Factory(DataStoreService.class).getService()

        if (!dsService) {
            throw new RuntimeException('DataStoreService konnte nicht erstellt werden.')
        }

        // 10 Einträge aus IssueRequest_Storage ziehen
        def entries = dsService.select('IssueRequest_Storage', 10)

        if (!entries || entries.isEmpty()) {
            messageLog?.addAttachmentAsString('Info', 'Keine Einträge in IssueRequest_Storage gefunden.', 'text/plain')
            return message       // nichts zu tun
        }

        // Bearbeitung aller ausgewählten Einträge
        entries.each { DataBean bean ->
            def payload = new String(bean.getDataAsArray(), StandardCharsets.UTF_8)

            try {
                processSinglePayload(payload, cfg, messageLog, dsService)
                // Nach erfolgreicher Verarbeitung Eintrag aus dem DataStore löschen
                dsService.delete('IssueRequest_Storage', bean.getHeaders()?.get('id') ?: bean.hashCode().toString())
            } catch(Exception e) {
                handleError(payload, e, messageLog)     // individuelle Fehlerbehandlung
            }
        }

        // Body für Monitoring-Zwecke setzen
        message.setBody('Processing finished for ' + entries.size() + ' entries')

    } catch(Exception e) {
        handleError(message.getBody(String) ?: '', e, messageLog)
    }

    return message
}

//==== 2. Initiale Konfiguration ========================================================
/*  Liest Properties aus dem Message-Objekt oder setzt Platzhalter                 */
def initConfig(Message msg) {

    def cfg = [:]

    cfg.requestUser             = msg.getProperty('requestUser')             ?: 'placeholder'
    cfg.requestPassword         = msg.getProperty('requestPassword')         ?: 'placeholder'
    cfg.requestURL              = msg.getProperty('requestURL')              ?: 'placeholder'
    cfg.digitalSignEnabled      = (msg.getProperty('digitalSignEnabled') ?: 'false').toString().toLowerCase() == 'true'
    cfg.plainHMACKeyForSignature= msg.getProperty('plainHMACKeyForSignature')?: 'gekmN8CFr6PngJ9xTGMOMpdTFAvcssg5'

    return cfg
}

//==== 3. Verarbeitung eines einzelnen Payloads =========================================
def processSinglePayload(String xml, Map cfg, def messageLog, DataStoreService dsService) {

    //--- 3.1 TaxNumber extrahieren & GoldenTax-Root filtern ----------------------------
    def slurper  = new XmlSlurper().parseText(xml)
    def taxNumber= slurper.'**'.find { it.name() == 'TaxNumber' }?.text() ?: 'placeholder'
    def goldenTaxXml = groovy.xml.XmlUtil.serialize(slurper)     // Root ist bereits <GoldenTax>

    //--- 3.2 Base64-Kodierung ----------------------------------------------------------
    def encodedPayload = goldenTaxXml.bytes.encodeBase64().toString()

    //--- 3.3 Optionales Signieren -------------------------------------------------------
    def signature = cfg.digitalSignEnabled ? createSignature(encodedPayload, cfg.plainHMACKeyForSignature) : null

    //--- 3.4 HTTP-POST an Steuerbehörde -----------------------------------------------
    def responseBody = callVatIssueService(cfg.requestURL, taxNumber, cfg.requestUser, cfg.requestPassword,
                                           encodedPayload, signature)

    //--- 3.5 Response verarbeiten ------------------------------------------------------
    handleResponse(responseBody, dsService, messageLog)
}

//==== 4. Signatur (HMAC-SHA256) ========================================================
/*  Signiert den übergebenen Text und liefert den Base64-String zurück             */
def createSignature(String data, String key) {

    Mac mac = Mac.getInstance('HmacSHA256')
    mac.init(new SecretKeySpec(key.bytes, 'HmacSHA256'))
    return mac.doFinal(data.bytes).encodeBase64().toString()
}

//==== 5. HTTP-Aufruf ===================================================================
/*  Führt den POST-Request durch und liefert den Response-Body als String zurück   */
def callVatIssueService(String baseUrl, String taxNumber, String user, String pwd,
                        String body, String signature) {

    def url = new URL("${baseUrl}/${taxNumber}")
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.with {
        requestMethod  = 'POST'
        doOutput       = true
        setRequestProperty('Content-Type', 'text/plain')
        def auth = "${user}:${pwd}".bytes.encodeBase64().toString()
        setRequestProperty('Authorization', "Basic ${auth}")
        if (signature) setRequestProperty('signature', signature)

        outputStream.withWriter { it << body }
    }

    def status = conn.responseCode
    def responseText = conn.inputStream.withReader { it.text }

    if (status != 200) {
        throw new RuntimeException("HTTP-Error: ${status} – ${conn.responseMessage}")
    }

    return responseText
}

//==== 6. Response-Handling =============================================================
def handleResponse(String respXml, DataStoreService dsService, def messageLog) {

    def resp = new XmlSlurper().parseText(respXml)
    def taxNo = resp.'**'.find { it.name() == 'TaxNumber' }?.text() ?: 'placeholder'

    //--- 6.1 pro GTHeader Status-Payload im DataStore ablegen --------------------------
    resp.'**'.findAll { it.name() == 'GTHeader' }.each { gtHeader ->
        def gtn  = gtHeader.GoldenTaxNumber.text()
        def stat = buildStatusPayload(gtn)

        writeToDataStore(dsService,
                          'GoldenTaxDocument_Result',
                          gtn,
                          stat,
                          true)
    }

    //--- 6.2 Mapping generieren & speichern -------------------------------------------
    def mappedXml = createMapping(resp, taxNo)

    writeToDataStore(dsService,
                     'QueryRequest_Storage',
                     taxNo,
                     mappedXml,
                     true)

    // optional fürs Monitoring
    messageLog?.addAttachmentAsString("Mapped_${taxNo}", mappedXml, 'text/xml')
}

//==== 7. Status-Payload erstellen ======================================================
def buildStatusPayload(String gtn) {

    return """<GoldenTax>
    <GoldenTaxNumber>${gtn}</GoldenTaxNumber>
    <CPIStatus>Q</CPIStatus>
    <OriginalAction>I</OriginalAction>
    <ReturnPayload>
        <InvoiceHeader>
            <OriginalAction>I</OriginalAction>
            <InvoiceStatus>P</InvoiceStatus>
            <GoldenTaxNumber>${gtn}</GoldenTaxNumber>
            <Messages>
                <ShortMessage>Invoice is pending with query process</ShortMessage>
            </Messages>
        </InvoiceHeader>
    </ReturnPayload>
</GoldenTax>"""
}

//==== 8. Mapping erstellen =============================================================
/*  Baut das Ziel-XML gem. Mapping-Anforderung                                      */
def createMapping(def resp, String taxNo) {

    def writer = new StringWriter()
    def xml    = new MarkupBuilder(writer)

    xml.GoldenTax {
        Control {
            TaxNumber   taxNo
            RetryCount  '1'

            resp.'**'.findAll { it.name() == 'GTHeader' }.each { gt ->
                MappingDetail {
                    GoldenTaxNumber gt.GoldenTaxNumber.text()
                    InvoiceType     gt.InvoiceType.text()
                    InvoiceMedium   gt.InvoiceMedium.text()
                }
            }
        }
    }
    return writer.toString()
}

//==== 9. DataStore-Schreibfunktion =====================================================
def writeToDataStore(DataStoreService service, String storeName, String id, String payload, boolean overwrite) {

    def dBean = new DataBean()
    dBean.setDataAsArray(payload.getBytes(StandardCharsets.UTF_8))

    def cfg = new DataConfig()
    cfg.setStoreName(storeName)
    cfg.setId(id)
    cfg.setOverwrite(overwrite)

    service.put(dBean, cfg)
}

//==== 10. Zentrales Fehler-Handling ====================================================
/*  Fügt fehlerhaften Payload als Attachment hinzu und wirft RuntimeException       */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im VAT-Issue-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}