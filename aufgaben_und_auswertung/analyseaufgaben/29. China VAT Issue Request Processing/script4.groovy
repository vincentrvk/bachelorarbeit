/****************************************************************************************
 *  SAP CPI – Groovy-Skript
 *  Verarbeitung von VAT-Issue-Requests für China Golden Tax
 *
 *  Autor: ChatGPT (Senior-Integration-Developer)
 *
 *  ACHTUNG:
 *  Dieses Skript ist als ONE-STOP-Skript konzipiert, das alle in der
 *  Aufgabenbeschreibung definierten Schritte ausführt.
 *  Es kann in einem Groovy-Script-Step einer IFlow-Schleife verwendet werden.
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.esb.datastore.DataStore
import com.sap.esb.datastore.Data
import groovy.xml.MarkupBuilder
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.util.Base64

/* ============================================================
 *  Globale Hilfsfunktion für das Error-Handling
 * ============================================================ */
def handleError(String payload, Exception e, def messageLog){
    messageLog?.addAttachmentAsString("ErrorPayload", payload ?: "", "text/xml")
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

/* ============================================================
 *  Haupt-Einstiegspunkt
 * ============================================================ */
Message processData(Message message){
    def messageLog   = messageLogFactory.getMessageLog(message)
    try{
        /* 1. Konfiguration lesen */
        def cfg = readConfiguration(message)

        /* 2. DataStore-Einträge laden (max. 10) */
        def dataStore  = getDataStore(message)
        def entries    = selectEntries(dataStore, "IssueRequest_Storage", 10)

        /* 3. Verarbeitung jeder einzelnen GoldenTax-Nachricht */
        entries.each{ Data dsEntry ->
            String payload           = new String(dsEntry.getDataAsArray(), StandardCharsets.UTF_8)
            String taxNumber         = extractTaxNumber(payload)
            cfg.taxNumber            = taxNumber      // dynamisch überschreiben
            String goldenTaxXml      = filterGoldenTax(payload)      // ggf. kappen
            String encodedPayload    = base64Encode(goldenTaxXml)
            String signature         = cfg.digitalSignEnabled.toBoolean() ? sign(encodedPayload,cfg.hmacKey) : null
            String responsePayload   = callIssueAPI(cfg, encodedPayload, signature)
            def   props              = extractProperties(responsePayload)

            /* 4. Daten im Ziel-DataStore ablegen                                   */
            props.each{ p -> storeResultDocument(dataStore, p) }

            /* 5. Response-Mapping erzeugen                                        */
            String mappedResponse    = buildResponseMapping(props, taxNumber)

            /* 6. Gemapptes Ergebnis speichern                                     */
            storeResponseMapping(dataStore, mappedResponse, taxNumber)

            /* 7. Logging                                                          */
            messageLog?.addAttachmentAsString("MappedResponse_${taxNumber}", mappedResponse, "text/xml")
        }

        /* Optional: leeren Dummy-Body setzen */
        message.setBody("<status>processing finished</status>")
        return message
    }catch(Exception e){
        handleError(message.getBody(java.lang.String) as String, e, messageLog)
    }
}

/* ============================================================
 *  Modul: Konfiguration einlesen
 * ============================================================ */
private Map readConfiguration(Message message){
    /*  Header / Properties in Map ablegen. Fallback auf „placeholder“        */
    def cfg = [:]
    cfg.user                 = (message.getProperty("requestUser")        ?: "placeholder") as String
    cfg.password             = (message.getProperty("requestPassword")    ?: "placeholder") as String
    cfg.requestUrl           = (message.getProperty("requestURL")         ?: "placeholder") as String
    cfg.digitalSignEnabled   = (message.getProperty("digitalSignEnabled") ?: "false")       as String
    cfg.hmacKey              = (message.getProperty("plainHMACKeyForSignature") ?: "gekmN8CFr6PngJ9xTGMOMpdTFAvcssg5") as String
    cfg.signatureHeaderName  = "signature"
    return cfg
}

/* ============================================================
 *  Modul: DataStore-Instanz ermitteln
 * ============================================================ */
private DataStore getDataStore(Message message){
    def camelCtx = message.exchange.context
    return camelCtx.registry.lookupByName(DataStore.class.getName()) as DataStore
}

/* ============================================================
 *  Modul: N Einträge aus DataStore lesen
 * ============================================================ */
private List<Data> selectEntries(DataStore ds, String storeName, int maxEntries){
    /*  Es werden nur die Einträge gezogen – kein Lösch-Handling hier         */
    return ds.select(storeName, maxEntries ?: 1) ?: []
}

/* ============================================================
 *  Modul: TaxNumber extrahieren
 * ============================================================ */
private String extractTaxNumber(String xml){
    def root = new XmlSlurper().parseText(xml)
    return root.Control.TaxNumber.text()
}

/* ============================================================
 *  Modul: GoldenTax-Node als Root filtern
 * ============================================================ */
private String filterGoldenTax(String xml){
    /*  Falls bereits GoldenTax Root, einfach zurückgeben                     */
    def root = new XmlSlurper().parseText(xml)
    return groovy.xml.XmlUtil.serialize(root)
}

/* ============================================================
 *  Modul: Base64-Kodierung
 * ============================================================ */
private String base64Encode(String data){
    return Base64.encoder.encodeToString(data.getBytes(StandardCharsets.UTF_8))
}

/* ============================================================
 *  Modul: Payload signieren (HMAC-SHA256)
 * ============================================================ */
private String sign(String payload, String key){
    Mac mac = Mac.getInstance("HmacSHA256")
    SecretKeySpec ks = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256")
    mac.init(ks)
    byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8))
    return Base64.encoder.encodeToString(raw)
}

/* ============================================================
 *  Modul: HTTP-POST „Issue“ API
 * ============================================================ */
private String callIssueAPI(Map cfg, String encodedPayload, String signature){
    URL url                     = new URL("${cfg.requestUrl}/${cfg.taxNumber}")
    def connection              = (HttpURLConnection) url.openConnection()
    connection.with{
        doOutput               = true
        requestMethod          = "POST"
        setRequestProperty("Authorization", "Basic " + ("${cfg.user}:${cfg.password}".bytes.encodeBase64().toString()))
        if(signature) setRequestProperty(cfg.signatureHeaderName, signature)
        setRequestProperty("Content-Type", "text/plain")
    }
    /*  Body schreiben                                                       */
    connection.outputStream.withWriter("UTF-8"){ it << encodedPayload }

    int rc = connection.responseCode
    if(rc == 200){
        return connection.inputStream.getText("UTF-8")
    }else{
        def errorText = connection.errorStream ? connection.errorStream.getText("UTF-8") : ""
        throw new RuntimeException("HTTP-Request fehlgeschlagen! Code=${rc}, Body=${errorText}")
    }
}

/* ============================================================
 *  Modul: Properties aus Response extrahieren
 * ============================================================ */
private List<Map> extractProperties(String responseXml){
    def root       = new XmlSlurper().parseText(responseXml)
    def taxNo      = root.TaxNumber.text()            ?: ""      // Fallback leer
    def list       = []
    root.Control.GTDocument.GTHeader.each{ hdr ->
        list << [
            taxNo           : taxNo,
            goldenTaxNumber : hdr.GoldenTaxNumber.text(),
            invoiceType     : hdr.InvoiceType.text(),
            invoiceMedium   : hdr.InvoiceMedium.text()
        ]
    }
    return list
}

/* ============================================================
 *  Modul: Dokument „GoldenTaxDocument_Result“ speichern
 * ============================================================ */
private void storeResultDocument(DataStore ds, Map props){
    def tpl = """<GoldenTax>
    <GoldenTaxNumber>${props.goldenTaxNumber}</GoldenTaxNumber>
    <CPIStatus>Q</CPIStatus>
    <OriginalAction>I</OriginalAction>
    <ReturnPayload>
        <InvoiceHeader>
            <OriginalAction>I</OriginalAction>
            <InvoiceStatus>P</InvoiceStatus>
            <GoldenTaxNumber>${props.goldenTaxNumber}</GoldenTaxNumber>
            <Messages>
                <ShortMessage>Invoice is pending with query process</ShortMessage>
            </Messages>
        </InvoiceHeader>
    </ReturnPayload>
</GoldenTax>"""

    Data data = new Data("GoldenTaxDocument_Result", props.goldenTaxNumber, tpl.getBytes(StandardCharsets.UTF_8))
    ds.put(data, true, false, 0, 0)     // overwrite=true, encrypt=false
}

/* ============================================================
 *  Modul: Response-Mapping aufbauen
 * ============================================================ */
private String buildResponseMapping(List<Map> propsList, String taxNumber){
    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)
    xml.GoldenTax{
        Control{
            TaxNumber(taxNumber)
            RetryCount("1")
            propsList.each{ p ->
                MappingDetail{
                    GoldenTaxNumber(p.goldenTaxNumber)
                    InvoiceType(p.invoiceType)
                    InvoiceMedium(p.invoiceMedium)
                }
            }
        }
    }
    return sw.toString()
}

/* ============================================================
 *  Modul: Gemappten Output im DataStore ablegen
 * ============================================================ */
private void storeResponseMapping(DataStore ds, String payload, String taxNumber){
    Data data = new Data("QueryRequest_Storage", taxNumber, payload.getBytes(StandardCharsets.UTF_8))
    ds.put(data, true, false, 0, 0)
}