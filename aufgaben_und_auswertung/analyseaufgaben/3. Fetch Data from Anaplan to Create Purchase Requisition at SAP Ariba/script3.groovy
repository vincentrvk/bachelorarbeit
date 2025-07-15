/************************************************************************************************
 * Groovy-Skript: Anaplan → SAP Ariba – Purchase-Request Import
 * Autor:        Senior-Developer (SAP Cloud Integration)
 * Beschreibung: Holt Purchase Requests (CSV) aus Anaplan, wandelt sie in das geforderte
 *               XML-Format um und sendet jede Request_ID einzeln an SAP Ariba.
 ************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.net.HttpURLConnection
import java.net.URL

//==============================================================
// Haupt-Einstieg
//==============================================================
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        // 1. Header/Property-Vorbereitung
        def cfg = buildConfigMap(message)

        // 2. GET – Anaplan CSV abrufen
        String csvPayload = getCsvFromAnaplan(cfg, messageLog)

        // 3. Validierung des Payloads
        csvPayload = validateCsv(csvPayload)

        // 4. Mapping CSV → XML-Bodies (gruppiert nach Request_ID)
        List<String> xmlBodies = mapCsvToXml(csvPayload)

        // 5. POST – jede Request_ID einzeln an Ariba senden
        xmlBodies.each { String xml ->
            String response = postToAriba(cfg, xml, messageLog)
            messageLog?.addAttachmentAsString("AribaResponse_${UUID.randomUUID()}", response, "text/plain")
        }

        // Letzten gesendeten Body als Message-Body setzen (rein informativ)
        if (xmlBodies) {
            message.setBody(xmlBodies.last())
        } else {
            message.setBody("")      // leerer String wenn kein Payload
        }
        return message

    } catch (Exception e) {
        handleError(message.getBody(String) ?: '', e, messageLog)
        return message          // wird nie erreicht (Exception), aber notwendig für Compiler
    }
}

//==============================================================
// Konfigurations-Funktion
//==============================================================
/*
 * Liest benötigte Header & Properties. Existiert ein Wert nicht, wird "placeholder" gesetzt.
 */
private Map buildConfigMap(Message msg) {
    Map<String, String> cfg = [:]
    ['SAP_Receiver', 'SAP_Sender', 'MessageID',
     'aribaURL', 'aribaUsername', 'aribaPassword',
     'anaplanURL', 'anaplanUsername', 'anaplanPassword'].each { key ->
        def val = msg.getProperty(key) ?: msg.getHeader(key, String.class)
        cfg[key] = (val ? val.toString() : 'placeholder')
    }
    return cfg
}

//==============================================================
// Anaplan-Aufruf
//==============================================================
/*
 * Führt einen HTTP-GET gegen die Anaplan-URL aus und liefert den CSV-String.
 */
private String getCsvFromAnaplan(Map cfg, def log) {
    URL url = new URL(cfg.anaplanURL)
    HttpURLConnection con = (HttpURLConnection) url.openConnection()
    con.with {
        requestMethod = 'GET'
        setConnectTimeout(15000)
        setReadTimeout(30000)
        String basicAuth = "${cfg.anaplanUsername}:${cfg.anaplanPassword}".bytes.encodeBase64().toString()
        setRequestProperty('Authorization', "Basic ${basicAuth}")
    }
    int rc = con.responseCode
    log?.addAttachmentAsString('AnaplanResponseCode', rc.toString(), 'text/plain')
    if (rc != HttpURLConnection.HTTP_OK) {
        throw new RuntimeException("GET Anaplan fehlgeschlagen. HTTP-Status: ${rc}")
    }
    return con.inputStream.getText('UTF-8')
}

//==============================================================
// CSV-Validierung
//==============================================================
/*
 * Entfernt Byte Order Mark, prüft auf leere Inhalte bzw. nur Header. Gibt ggf. leeren String zurück.
 */
private String validateCsv(String csv) {
    if (csv == null) { return '' }
    // BOM entfernen
    csv = csv.replaceAll('^\ufeff', '').trim()
    // Leer oder nur Header?
    String headerLine =
            'PR_Creation_List,PR_PO_Status,Created_By,Request_ID,PR_Number,Unique_Key,' +
            'Request_Name,Contract_Reference,Anaplan_Line_Number,PR_Line_Number,' +
            'Quantity,Price,Supplier_Code,SKU,Deliver_To_Code,Need_By_Date'
    def lines = csv.readLines()
    if (!lines || (lines.size() == 1 && lines[0].equalsIgnoreCase(headerLine))) {
        return ''
    }
    return csv
}

//==============================================================
// Mapping CSV → XML
//==============================================================
/*
 * Parst die CSV grob (keine Quote-Unterstützung) und gruppiert nach Request_ID.
 * Liefert eine Liste von XML-Strings (ein XML pro Request_ID).
 */
private List<String> mapCsvToXml(String csv) {
    if (!csv) { return [] }

    def rows = []
    def headers = []
    csv.readLines().eachWithIndex { line, idx ->
        def cols = line.split(',', -1).collect { it.trim() }
        if (idx == 0) {
            headers = cols
        } else {
            Map<String, String> row = [:]
            headers.eachWithIndex { h, i -> row[h] = (i < cols.size() ? cols[i] : '') }
            rows << row
        }
    }
    // Gruppieren nach Request_ID
    def grouped = rows.groupBy { it.Request_ID }

    // Für jede Gruppe XML bauen
    List<String> xmlList = []
    grouped.each { reqId, list ->
        def sw = new StringWriter()
        def xml = new MarkupBuilder(sw)
        xml.PURCHASE_REQUISITION {
            REQUEST_ID(reqId)
            LINES {
                list.each { ln ->
                    PURCHASE_REQUISITION_LINE {
                        LINE_NO(ln.Anaplan_Line_Number)
                        BUY_UNIT_PRICE(ln.Price)
                        PR_PO_Status(ln.PR_PO_Status.equalsIgnoreCase('Change') ? 'Update' : ln.PR_PO_Status)
                        CURRENCY('')
                    }
                }
            }
        }
        xmlList << sw.toString()
    }
    return xmlList
}

//==============================================================
// Ariba-Aufruf
//==============================================================
/*
 * Sendet einen XML-Body via POST an SAP Ariba und gibt die Response als String zurück.
 */
private String postToAriba(Map cfg, String xmlBody, def log) {
    URL url = new URL(cfg.aribaURL)
    HttpURLConnection con = (HttpURLConnection) url.openConnection()
    con.with {
        requestMethod = 'POST'
        doOutput      = true
        setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
        setConnectTimeout(15000)
        setReadTimeout(30000)
        String basicAuth = "${cfg.aribaUsername}:${cfg.aribaPassword}".bytes.encodeBase64().toString()
        setRequestProperty('Authorization', "Basic ${basicAuth}")
    }
    con.outputStream.withWriter('UTF-8') { it << xmlBody }
    int rc = con.responseCode
    def respStream = (rc >= 200 && rc < 300) ? con.inputStream : con.errorStream
    String resp = respStream?.getText('UTF-8') ?: ''
    if (rc < 200 || rc >= 300) {
        throw new RuntimeException("POST Ariba fehlgeschlagen. HTTP-Status: ${rc} – Payload: ${resp}")
    }
    return resp
}

//==============================================================
// Error-Handling
//==============================================================
/*
 * Einheitliche Fehlerbehandlung: Payload als Attachment, Log-Eintrag & Exception-Throw.
 */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/plain")
    def errorMsg = "Fehler im Purchase-Request-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}