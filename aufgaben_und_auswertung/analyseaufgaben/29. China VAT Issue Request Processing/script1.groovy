/***************************************************************************
 * Groovy–Skript – SAP Cloud Integration
 *
 * Verarbeitung von Mehrwertsteuer-Requests für die chinesischen Steuerbehörden
 * (Document & Reporting Compliance – Golden Tax).
 *
 * Autor: AI-Assistant
 ***************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.esb.datastore.DataStore
import com.sap.esb.datastore.Data
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.util.Base64

//======================================================
//  Haupt-Einstiegspunkt
//======================================================
Message processData(Message message) {

    try {

        /*------------------------------------------------------------------
         * 0. Initialisierung
         *----------------------------------------------------------------*/
        setInitialContext(message)

        /*------------------------------------------------------------------
         * 1. Requests aus Datastore lesen (max. 10 Elemente)
         *----------------------------------------------------------------*/
        DataStore ds        = getDataStore(message)
        List<Data> reqList  = retrieveRequests(ds    ,
                                               "IssueRequest_Storage",
                                               10)

        if (reqList.isEmpty()) {
            message.setBody("Keine Einträge im Datastore ‚IssueRequest_Storage‘.")
            return message
        }

        /*------------------------------------------------------------------
         * 2. Verarbeitung jedes Einzelelements
         *----------------------------------------------------------------*/
        List<String> mappingOutputs = []

        reqList.each { Data entry ->
            String payload        = new String(entry.getDataAsArray(),
                                               StandardCharsets.UTF_8)

            // 2.1 Tax-Number ermitteln
            String taxNumber      = extractTaxNumber(payload)
            message.setProperty("taxNumber", taxNumber)

            // 2.2 Payload filtern (Root = <GoldenTax>)
            String filtered       = filterGoldenTaxRoot(payload)

            // 2.3 Base64-Kodierung
            String encoded        = Base64.encoder.encodeToString(
                                        filtered.getBytes(StandardCharsets.UTF_8))

            // 2.4 Signatur (optional)
            String signature      = signIfRequired(encoded,
                                                   message.getProperty("digitalSignEnabled") as String,
                                                   message.getProperty("plainHMACKeyForSignature") as String)

            // 2.5 Request absetzen
            String responseBody   = postVatIssue(encoded,
                                                 signature,
                                                 taxNumber,
                                                 message)

            // 2.6 Benötigte Properties aus Antwort extrahieren
            Map<String, String> props = extractPropertiesFromResponse(responseBody)
            // 2.6.1  Ergebnis-Payload pro GT-Header im Datastore ablegen
            props.goldenTaxNumbers.eachWithIndex { String gtn, int idx ->
                String payloadResult = buildGoldenTaxDocumentResult(gtn)
                writeToDataStore(ds,
                                 "GoldenTaxDocument_Result",
                                 gtn,
                                 payloadResult)
            }

            // 2.7 Mapping
            String mappedPayload  = mapResponsePayload(props,
                                                        message.getProperty("taxNo") as String)
            // 2.8 Mapping–Ergebnis speichern
            writeToDataStore(ds,
                             "QueryRequest_Storage",
                             props.taxNo,
                             mappedPayload)

            mappingOutputs << mappedPayload

            // 2.9 Ursprungs-Eintrag entfernen (verarbeitet)
            ds.delete("IssueRequest_Storage", entry.getId())
        }

        /*------------------------------------------------------------------
         * 3. Abschließendes Ergebnis in Message-Body
         *----------------------------------------------------------------*/
        message.setBody(mappingOutputs.join("\n"))
        return message

    } catch (Exception ex) {
        // zentrales Error-Handling
        return handleError(message, ex)
    }
}

/*=====================================================
 *               Hilfs-Funktionen
 *===================================================*/

/**
 * Liest Properties und Header aus dem Message-Objekt
 * und legt sie – ggf. mit Platzhalterwerten – als Message
 * Properties ab.
 */
def setInitialContext(Message msg) {
    ["requestUser"              ,
     "requestPassword"          ,
     "requestURL"               ,
     "digitalSignEnabled"       ,
     "plainHMACKeyForSignature"].each { String key ->
        def val = msg.getProperty(key)
        if (val == null) {
            val = msg.getHeader(key, String.class)
        }
        msg.setProperty(key, val ?: "placeholder")
    }
}

/**
 * Liefert die DataStore-Instanz aus dem Camel-Context.
 */
DataStore getDataStore(Message msg) {
    def camelCtx = msg.exchange.context
    (DataStore) camelCtx.registry.lookupByName(DataStore.class.getName())
}

/**
 * Holt bis zu ‹batchSize› Einträge aus dem angegebenen Datastore.
 */
List<Data> retrieveRequests(DataStore ds, String storeName, int batchSize) {
    try {
        ds.select(storeName, batchSize) ?: []
    } catch (Exception ex) {
        throw new RuntimeException(
            "Fehler beim Lesen aus Datastore '${storeName}': ${ex.message}", ex)
    }
}

/**
 * Extrahiert die Steuer-Nummer.
 */
String extractTaxNumber(String xml) {
    def slurper   = new XmlSlurper().parseText(xml)
    String taxNum = slurper.Control.TaxNumber.text()
    if (!taxNum) {
        throw new IllegalStateException("TaxNumber nicht gefunden!")
    }
    taxNum
}

/**
 * Liefert XML-String, dessen Root-Element <GoldenTax> ist.
 */
String filterGoldenTaxRoot(String xml) {
    def node = new XmlSlurper().parseText(xml)
    groovy.xml.XmlUtil.serialize(node)
}

/**
 * Signiert den kodierten Payload, wenn aktiviert.
 * Rückgabe: Signatur oder null.
 */
String signIfRequired(String encodedPayload,
                      String enabled,
                      String key) {

    if ("true".equalsIgnoreCase(enabled)) {
        Mac mac = Mac.getInstance("HmacSHA256")
        mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8),
                                   "HmacSHA256"))
        byte[] raw = mac.doFinal(encodedPayload.getBytes(StandardCharsets.UTF_8))
        return Base64.encoder.encodeToString(raw)
    }
    return null
}

/**
 * Sendet den POST VAT Issue Request.
 */
String postVatIssue(String payloadEncoded,
                    String signature,
                    String taxNumber,
                    Message message) {

    String baseUrl  = message.getProperty("requestURL") as String
    String user     = message.getProperty("requestUser") as String
    String pwd      = message.getProperty("requestPassword") as String
    String url      = "${baseUrl}/${taxNumber}"

    URL connUrl     = new URL(url)
    HttpURLConnection con = (HttpURLConnection) connUrl.openConnection()
    con.setRequestMethod("POST")
    String auth     = Base64.encoder.encodeToString("${user}:${pwd}".getBytes(
                                                   StandardCharsets.UTF_8))
    con.setRequestProperty("Authorization", "Basic ${auth}")
    con.setRequestProperty("Content-Type" , "text/plain")
    if (signature) {
        con.setRequestProperty("signature", signature)
    }
    con.setDoOutput(true)
    con.outputStream.withWriter("UTF-8") { it << payloadEncoded }

    int rc = con.responseCode
    if (rc != 200) {
        throw new RuntimeException("HTTP-Fehler ${rc} bei Aufruf ${url}")
    }
    con.inputStream.getText("UTF-8")
}

/**
 * Extrahiert benötigte Properties aus der Response.
 * Rückgabewert:
 *   taxNo:             String
 *   goldenTaxNumbers:  List<String>
 *   invoiceTypes:      List<String>
 *   invoiceMediums:    List<String>
 */
Map<String, Object> extractPropertiesFromResponse(String xml) {
    def resp        = new XmlSlurper().parseText(xml)
    String taxNo    = resp.TaxNumber.text() ?:
                      resp.Control.TaxNumber.text()
    List<String> gtn= []
    List<String> it = []
    List<String> im = []

    resp.'**'.findAll { it.name() == 'GTHeader' }.each { hdr ->
        gtn << hdr.GoldenTaxNumber.text()
        it  << hdr.InvoiceType.text()
        im  << hdr.InvoiceMedium.text()
    }

    [taxNo: taxNo,
     goldenTaxNumbers: gtn,
     invoiceTypes     : it,
     invoiceMediums   : im]
}

/**
 * Baut den Payload für GoldenTaxDocument_Result.
 */
String buildGoldenTaxDocumentResult(String goldenTaxNumber) {
    """<GoldenTax>
    <GoldenTaxNumber>${goldenTaxNumber}</GoldenTaxNumber>
    <CPIStatus>Q</CPIStatus>
    <OriginalAction>I</OriginalAction>
    <ReturnPayload>
        <InvoiceHeader>
            <OriginalAction>I</OriginalAction>
            <InvoiceStatus>P</InvoiceStatus>
            <GoldenTaxNumber>${goldenTaxNumber}</GoldenTaxNumber>
            <Messages>
                <ShortMessage>Invoice is pending with query process</ShortMessage>
            </Messages>
        </InvoiceHeader>
    </ReturnPayload>
</GoldenTax>"""
}

/**
 * Schreibt einen Eintrag in den angegebenen Datastore.
 */
void writeToDataStore(DataStore ds,
                      String storeName,
                      String entryId,
                      String payload) {

    try {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8)
        Data data    = new Data(storeName, entryId, bytes)
        ds.put(data, true, false, 0L, 0L)
    } catch (Exception ex) {
        throw new RuntimeException(
            "Fehler beim Schreiben in Datastore '${storeName}', " +
            "EntryID '${entryId}': ${ex.message}", ex)
    }
}

/**
 * Erstellt das finale Mapping gemäß Vorgabe.
 */
String mapResponsePayload(Map props, String taxNoFallback) {

    def writer = new StringWriter()
    def builder = new groovy.xml.MarkupBuilder(writer)

    builder.GoldenTax {
        Control {
            TaxNumber(props.taxNo ?: taxNoFallback)
            RetryCount('1')
            props.goldenTaxNumbers.eachWithIndex { String gtn, int idx ->
                MappingDetail {
                    GoldenTaxNumber(gtn)
                    InvoiceType(props.invoiceTypes[idx])
                    InvoiceMedium(props.invoiceMediums[idx])
                }
            }
        }
    }
    writer.toString()
}

/**
 * Zentrales Error-Handling. Fügt den Payload als Attachment hinzu.
 */
Message handleError(Message msg, Exception ex) {
    def msgLog = messageLogFactory.getMessageLog(msg)
    String body = msg.getBody(String) ?: ""
    msgLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    msgLog?.addAttachmentAsString("Exception", ex.toString(), "text/plain")
    throw new RuntimeException("Fehler im Groovy-Skript: ${ex.message}", ex)
}