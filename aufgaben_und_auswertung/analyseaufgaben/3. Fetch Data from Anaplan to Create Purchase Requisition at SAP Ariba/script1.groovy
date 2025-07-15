/*
 *  Groovy-Skript – Anaplan Purchase-Request Import zu SAP Ariba
 *
 *  Hinweis:
 *  1. Jede Aufgabe ist in eine eigene Methode ausgelagert (Modularität).
 *  2. Sämtliche Methoden enthalten deutschsprachige Kommentare.
 *  3. An jeder relevanten Stelle werden Ausnahmen abgefangen und aussagekräftig
 *     geloggt. Zusätzlich wird der eingehende Payload als Attachment weitergegeben.
 *  4. Es werden keine globalen Variablen oder Konstanten verwendet.
 */
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.net.HttpURLConnection
import java.nio.charset.StandardCharsets

/* Haupteinstieg für das CPI-Skript */
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1. Header & Properties vorbereiten */
        def ctx = prepareContext(message)

        /* 2. GET-Call zu Anaplan */
        String csvPayload = getAnaplanData(ctx, messageLog)

        /* 3. CSV-Payload validieren */
        csvPayload = validateCsvPayload(csvPayload)
        if (csvPayload.isEmpty()) {
            message.setBody("")                          // Leere Nachricht zurückgeben
            return message
        }

        /* 4. CSV → XML mappen */
        List<String> xmlBodies = mapCsvToXml(csvPayload)

        /* 5. POST-Aufruf für jeden XML-Body an Ariba */
        sendToAriba(xmlBodies, ctx, messageLog)

        /* Optional: Den (letzten) gesendeten XML-Body in den Message-Body legen,
           sodass er im Monitoring sofort ersichtlich ist                        */
        message.setBody(xmlBodies.last())

        return message

    } catch (Exception e) {
        /* Zentrales Error-Handling */
        return handleError(message, e, messageLog)
    }
}

/* ========================================================================== */
/*                              Hilfsmethoden                                 */
/* ========================================================================== */

/* Liest benötigte Header & Properties oder setzt 'placeholder' */
private Map<String, String> prepareContext(Message msg) {
    [
            anaplanURL   : getValue(msg.getProperty("anaplanURL")),
            anaplanUser  : getValue(msg.getProperty("anaplanUsername")),
            anaplanPwd   : getValue(msg.getProperty("anaplanPassword")),
            aribaURL     : getValue(msg.getProperty("aribaURL")),
            aribaUser    : getValue(msg.getProperty("aribaUsername")),
            aribaPwd     : getValue(msg.getProperty("aribaPassword")),
            messageID    : getValue(msg.getProperty("MessageID")),
            sapSender    : getValue(msg.getProperty("SAP_Sender")),
            sapReceiver  : getValue(msg.getProperty("SAP_Receiver"))
    ]
}

/* Gibt entweder den gefundenen Wert oder 'placeholder' zurück */
private String getValue(Object val) {
    return (val ? val.toString() : "placeholder")
}

/* GET-Aufruf an Anaplan zum Abrufen des CSV-Payloads */
private String getAnaplanData(Map ctx, def log) {

    HttpURLConnection conn
    try {
        conn = (HttpURLConnection) new URL(ctx.anaplanURL).openConnection()
        conn.requestMethod      = "GET"
        conn.connectTimeout     = 30000
        conn.readTimeout        = 60000
        conn.doInput            = true
        conn.setRequestProperty("Authorization",
                "Basic ${("${ctx.anaplanUser}:${ctx.anaplanPwd}")
                        .getBytes(StandardCharsets.UTF_8)
                        .encodeBase64().toString()}")

        int rc = conn.responseCode
        String body = rc == 200  ?
                conn.inputStream.getText("UTF-8") :
                conn.errorStream?.getText("UTF-8")

        log?.addAttachmentAsString("AnaplanResponse_${ctx.messageID}",
                body ?: "", "text/plain")

        if (rc != 200) {
            throw new RuntimeException("Anaplan-Aufruf fehlschlagend – HTTP ${rc}")
        }
        return body ?: ""

    } finally {
        conn?.disconnect()
    }
}

/* Prüft, ob der CSV-Payload verwertbare Daten enthält */
private String validateCsvPayload(String csv) {

    if (!csv) {
        return ""
    }

    csv = csv.trim()
    if (csv.isEmpty()) {
        return ""
    }

    /* Header-Zeile laut Anforderung */
    final String CSV_HEADER =
            "PR_Creation_List,PR_PO_Status,Created_By,Request_ID,PR_Number," +
            "Unique_Key,Request_Name,Contract_Reference,Anaplan_Line_Number," +
            "PR_Line_Number,Quantity,Price,Supplier_Code,SKU,Deliver_To_Code,Need_By_Date"

    def lines = csv.split(/\r?\n/)
    if (lines.size() <= 1) {
        return ""                                      // nur Header oder leer
    }
    if (lines.size() == 2 && lines[0] == CSV_HEADER && lines[1].trim().isEmpty()) {
        return ""                                      // Header + Leerzeile
    }
    return csv
}

/* Mapping von CSV → Liste XML-Bodies (1 pro Request_ID) */
private List<String> mapCsvToXml(String csv) {

    def lines       = csv.split(/\r?\n/)
    def headers     = lines[0].split(",", -1)
    def dataLines   = lines.tail().findAll { it.trim() }

    /* Daten nach Request_ID gruppieren */
    Map<String, List<Map<String, String>>> grouped = [:]
    dataLines.each { l ->
        def cols   = l.split(",", -1)
        def record = [:]
        headers.eachWithIndex { h, idx -> record[h] = idx < cols.size() ? cols[idx] : "" }

        def reqId = record["Request_ID"]
        grouped.computeIfAbsent(reqId) { [] } << record
    }

    /* Für jede Request_ID XML erzeugen */
    List<String> xmlBodies = []
    grouped.each { reqId, records ->

        StringWriter sw = new StringWriter()
        MarkupBuilder mb = new MarkupBuilder(sw)

        mb mkp.declareNamespace("")                    // verhindert xmlns=""
        mb."PURCHASE_REQUISITION" {
            REQUEST_ID(reqId)
            LINES {
                records.each { r ->
                    "PURCHASE_REQUISITION_LINE" {
                        LINE_NO          (r["Anaplan_Line_Number"])
                        BUY_UNIT_PRICE   (r["Price"])
                        def status = r["PR_PO_Status"].equalsIgnoreCase("Change") ?
                                "Update" : r["PR_PO_Status"]
                        PR_PO_Status     (status)
                        CURRENCY         ("")
                    }
                }
            }
        }

        xmlBodies << """<?xml version="1.0" encoding="UTF-8"?>\n${sw.toString()}"""
    }

    return xmlBodies
}

/* POST-Aufruf an Ariba für jeden einzelnen XML-Body */
private void sendToAriba(List<String> xmlBodies, Map ctx, def log) {

    int counter = 0
    xmlBodies.each { xml ->
        counter++

        HttpURLConnection conn
        try {
            conn = (HttpURLConnection) new URL(ctx.aribaURL).openConnection()
            conn.requestMethod      = "POST"
            conn.doOutput           = true
            conn.connectTimeout     = 30000
            conn.readTimeout        = 60000
            conn.setRequestProperty("Content-Type", "application/xml; charset=UTF-8")
            conn.setRequestProperty("Authorization",
                    "Basic ${("${ctx.aribaUser}:${ctx.aribaPwd}")
                            .getBytes(StandardCharsets.UTF_8)
                            .encodeBase64().toString()}")

            conn.outputStream.withWriter("UTF-8") { it << xml }

            int rc       = conn.responseCode
            String resp  = rc < 400 ? conn.inputStream.getText("UTF-8")
                                    : conn.errorStream?.getText("UTF-8")

            /* Request & Response anhängen */
            log?.addAttachmentAsString("AribaRequest_${counter}",  xml,  "application/xml")
            log?.addAttachmentAsString("AribaResponse_${counter}", resp ?: "", "text/plain")

            if (rc >= 400) {
                throw new RuntimeException("Ariba-Aufruf fehlgeschlagen – HTTP ${rc}")
            }

        } finally {
            conn?.disconnect()
        }
    }
}

/* Zentrales Error-Handling – erstellt Attachment & wirft RuntimeException */
private Message handleError(Message msg, Exception e, def log) {

    String bodyString
    try {
        bodyString = msg.getBody(String) ?: ""
    } catch (Exception ignored) {
        bodyString = ""
    }

    log?.addAttachmentAsString("ErrorPayload", bodyString, "text/plain")
    def errMsg = "Fehler im Import-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}