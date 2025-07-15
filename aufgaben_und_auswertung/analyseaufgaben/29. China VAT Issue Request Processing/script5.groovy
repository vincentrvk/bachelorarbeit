/****************************************************************************
 *  SAP CPI – Groovy-Skript                                                   *
 *  VAT Issue Integration – Golden Tax China                                  *
 *                                                                           *
 *  Hinweis:                                                                 *
 *  1.  Erfüllt alle in der Aufgabenstellung genannten Teilaufgaben.         *
 *  2.  Modularer Aufbau mit deutschsprachigen Funktions-Dok-Comments.       *
 *  3.  Zentrales, einheitliches Error-Handling.                              *
 ****************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory
import com.sap.esb.datastore.DataStore
import com.sap.esb.datastore.Data
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.net.HttpURLConnection
import java.nio.charset.StandardCharsets
import groovy.xml.MarkupBuilder

/* =========================================================================
 *  Haupt-Einstiegspunkt der Script-Ausführung
 * ========================================================================= */
Message processData(Message message) {

    def msgLog = messageLogFactory.getMessageLog(message)
    try {
        /* -----------------------------------------------------------------
         *  1. Konfiguration (Header & Properties einlesen bzw. Defaults)
         * ---------------------------------------------------------------- */
        def cfg = buildConfiguration(message)

        /* -----------------------------------------------------------------
         *  2. Daten aus dem DataStore "IssueRequest_Storage" lesen
         * ---------------------------------------------------------------- */
        def camelCtx = message.exchange.context
        DataStore dataStore = camelCtx.registry.lookupByName(DataStore.class.getName()) as DataStore
        List<Data> dsEntries = dataStore?.select("IssueRequest_Storage", 10) ?: []

        if (dsEntries.isEmpty()) {
            msgLog?.addAttachmentAsString("Info", "Keine Einträge im IssueRequest_Storage gefunden.", "text/plain")
            return message
        }

        /* -----------------------------------------------------------------
         *  3. Verarbeitung jedes einzelnen Payloads
         * ---------------------------------------------------------------- */
        dsEntries.each { Data ds ->
            def originalPayload = new String(ds.getDataAsArray(), StandardCharsets.UTF_8)

            try {
                processSingleMessage(originalPayload, cfg, dataStore, msgLog)
                /* Nach erfolgreicher Verarbeitung Eintrag aus DataStore löschen */
                dataStore.delete("IssueRequest_Storage", ds.id)
            } catch (Exception ex) {
                handleError(originalPayload, ex, msgLog)          // Fehlerbehandlung mit Attachment
            }
        }

        message.setBody("Es wurden ${dsEntries.size()} Datensätze verarbeitet.")
        return message

    } catch (Exception e) {
        /* Unerwarteter Fehler im Gesamtablauf */
        handleError(message.getBody(String) as String, e, msgLog)
        return message   // wird niemals erreicht, handleError wirft Exception
    }
}

/* =========================================================================
 *  Funktions-Definitionen (Modularer Aufbau)
 * ========================================================================= */

/* -------------------------------------------------------------------------
 *  buildConfiguration
 *  - Liest notwendige Header / Properties oder verwendet Platzhalter.
 * ----------------------------------------------------------------------- */
private Map buildConfiguration(Message message) {
    [
            requestUser              : (message.getProperty("requestUser")      ?: "placeholder") as String,
            requestPassword          : (message.getProperty("requestPassword")  ?: "placeholder") as String,
            requestURL               : (message.getProperty("requestURL")       ?: "placeholder") as String,
            digitalSignEnabled       : (message.getProperty("digitalSignEnabled")?: "false")      as String,
            plainHMACKeyForSignature : (message.getProperty("plainHMACKeyForSignature")
                    ?: "gekmN8CFr6PngJ9xTGMOMpdTFAvcssg5") as String
    ]
}

/* -------------------------------------------------------------------------
 *  processSingleMessage
 *  - Führt alle Einzelschritte für genau einen ursprünglichen Payload aus.
 * ----------------------------------------------------------------------- */
private void processSingleMessage(String sourceXml,
                                  Map cfg,
                                  DataStore dataStore,
                                  def msgLog) {

    /* 4. Golden Tax Number / Steuernummer extrahieren */
    def parser = new XmlSlurper().parseText(sourceXml)
    String taxNumber = parser.Control?.TaxNumber?.text() ?: "UNKNOWN"

    /* 5. Filter-Schritt ist bereits erfüllt, da <GoldenTax> Root-Element ist */

    /* 6. Base64-Kodierung */
    String encodedPayload = sourceXml.bytes.encodeBase64().toString()

    /* 7. (Optional) HMAC-Signatur */
    String signature = ""
    if (cfg.digitalSignEnabled.equalsIgnoreCase("true")) {
        signature = createHmacSha256Signature(encodedPayload, cfg.plainHMACKeyForSignature)
    }

    /* 8. Externer POST-Aufruf */
    String responseXml = callVatIssueAPI(encodedPayload, signature, taxNumber, cfg, msgLog)

    /* 9. Response-Properties extrahieren & 10. GoldenTaxDocument_Result ablegen */
    List<Map> headerList = persistResultPerHeader(responseXml, dataStore, msgLog)

    /* 11. Mapping durchführen */
    String mappedPayload = buildResponseMapping(taxNumber, headerList)

    /* 12. Gemappten Payload im QueryRequest_Storage speichern */
    writeToDataStore("QueryRequest_Storage", taxNumber, mappedPayload, dataStore)
}

/* -------------------------------------------------------------------------
 *  createHmacSha256Signature
 *  - Signiert einen String mittels HMAC-SHA256 und Base64-encodiert das
 *    Ergebnis (wird als Header "signature" versendet).
 * ----------------------------------------------------------------------- */
private String createHmacSha256Signature(String data, String key) {
    SecretKeySpec secretKey = new SecretKeySpec(key.bytes, "HmacSHA256")
    Mac mac = Mac.getInstance("HmacSHA256")
    mac.init(secretKey)
    byte[] rawHmac = mac.doFinal(data.bytes)
    return rawHmac.encodeBase64().toString()
}

/* -------------------------------------------------------------------------
 *  callVatIssueAPI
 *  - Führt den HTTP-POST an die chinesische Steuerbehörde aus.
 * ----------------------------------------------------------------------- */
private String callVatIssueAPI(String encodedPayload,
                               String signature,
                               String taxNumber,
                               Map cfg,
                               def msgLog) {

    HttpURLConnection conn = null
    try {
        URL url = new URL("${cfg.requestURL}/${taxNumber}")
        conn = (HttpURLConnection) url.openConnection()
        conn.with {
            requestMethod = "POST"
            doOutput = true
            setRequestProperty("Content-Type", "text/plain")
            /* Basic Authentication */
            String basicAuth = "${cfg.requestUser}:${cfg.requestPassword}".bytes.encodeBase64().toString()
            setRequestProperty("Authorization", "Basic $basicAuth")
            /* Signaturheader setzen, falls vorhanden */
            if (signature) {
                setRequestProperty("signature", signature)
            }
            /* Payload senden */
            outputStream.withWriter("UTF-8") { writer ->
                writer << encodedPayload
            }
        }

        int status = conn.responseCode
        msgLog?.addAttachmentAsString("HTTP-Status", status.toString(), "text/plain")

        String respBody = ""
        if (status == 200) {
            respBody = conn.inputStream.getText("UTF-8")
        } else {
            String err = conn?.errorStream ? conn.errorStream.getText("UTF-8") : "No error payload"
            throw new RuntimeException("HTTP-Fehler ${status}: ${err}")
        }
        return respBody

    } finally {
        conn?.disconnect()
    }
}

/* -------------------------------------------------------------------------
 *  persistResultPerHeader
 *  - Legt pro <GTHeader> ein Status-Payload im DataStore
 *    "GoldenTaxDocument_Result" ab und liefert eine Liste mit Header-Infos
 *    für das spätere Mapping zurück.
 * ----------------------------------------------------------------------- */
private List<Map> persistResultPerHeader(String responseXml,
                                         DataStore dataStore,
                                         def msgLog) {

    def resp = new XmlSlurper().parseText(responseXml)
    def headers = resp.Control?.GTDocument?.GTHeader
    List<Map> headerList = []

    headers.each { gt ->
        String gtn          = gt.GoldenTaxNumber.text()
        String invType      = gt.InvoiceType.text()
        String invMedium    = gt.InvoiceMedium.text()

        /* Status-Payload (gemäß Vorgabe) erzeugen */
        def sw = new StringWriter()
        def xml = new MarkupBuilder(sw)
        xml.GoldenTax {
            GoldenTaxNumber(gtn)
            CPIStatus("Q")
            OriginalAction("I")
            ReturnPayload {
                InvoiceHeader {
                    OriginalAction("I")
                    InvoiceStatus("P")
                    GoldenTaxNumber(gtn)
                    Messages {
                        ShortMessage("Invoice is pending with query process")
                    }
                }
            }
        }
        writeToDataStore("GoldenTaxDocument_Result", gtn, sw.toString(), dataStore)

        headerList << [gtn: gtn, invType: invType, invMedium: invMedium]
    }
    return headerList
}

/* -------------------------------------------------------------------------
 *  buildResponseMapping
 *  - Erstellt das Ziel-XML laut Mapping-Anforderung.
 * ----------------------------------------------------------------------- */
private String buildResponseMapping(String taxNo, List<Map> headerList) {

    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)
    xml.GoldenTax {
        TaxNumber(taxNo)
        RetryCount("1")
        headerList.each { hdr ->
            MappingDetail {
                GoldenTaxNumber(hdr.gtn)
                InvoiceType(hdr.invType)
                InvoiceMedium(hdr.invMedium)
            }
        }
    }
    return sw.toString()
}

/* -------------------------------------------------------------------------
 *  writeToDataStore
 *  - Schreibt einen Payload als neuen Eintrag in den übergebenen DataStore.
 * ----------------------------------------------------------------------- */
private void writeToDataStore(String storeName,
                              String entryId,
                              String payload,
                              DataStore dataStore) {

    byte[] data = payload.getBytes(StandardCharsets.UTF_8)
    Data dsData = new Data(storeName, entryId, data)
    /* overwrite=true, encrypt=false, alertPeriod=0, expirePeriod=0 */
    dataStore.put(dsData, true, false, 0L, 0L)
}

/* -------------------------------------------------------------------------
 *  handleError  (vorgegebenes Snippet erweitert)
 *  - Einheitliches Error-Handling mit Attachment des fehlerhaften Payloads.
 * ----------------------------------------------------------------------- */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "Kein Body vorhanden", "text/xml")
    String errorMsg = "Fehler im Integration-Skript: ${e.message}"
    messageLog?.addAttachmentAsString("StackTrace", getStackTrace(e), "text/plain")
    throw new RuntimeException(errorMsg, e)
}

/*  Hilfsfunktion – Stacktrace als String zurückgeben */
private static String getStackTrace(Throwable t) {
    StringWriter sw = new StringWriter()
    t.printStackTrace(new PrintWriter(sw))
    return sw.toString()
}