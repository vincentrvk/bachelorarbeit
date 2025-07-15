/******************************************************************************
 * Groovy-Skript für SAP Cloud Integration
 * Import von Purchase-Requests aus Anaplan und Versand an SAP Ariba
 * Autor: ChatGPT (Senior-Entwickler)
 ******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.net.HttpURLConnection
import java.net.URL

/**************************************
 * Haupt-Einstiegspunkt des Skriptes  *
 **************************************/
Message processData(Message message) {

    // MessageLog für Debugging/Historie
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* ------------------------------------------------------------
         * 1) Header & Properties auslesen / belegen
         * ---------------------------------------------------------- */
        def cfg = setHeadersAndProperties(message)


        /* ------------------------------------------------------------
         * 2) GET-Call an Anaplan (CSV-Import abrufen)
         * ---------------------------------------------------------- */
        String csvPayload = callGetFromAnaplan(cfg.anaplanURL,
                                               cfg.anaplanUser,
                                               cfg.anaplanPwd,
                                               messageLog)

        /* ------------------------------------------------------------
         * 3) Validierung des CSV-Payloads
         * ---------------------------------------------------------- */
        String validatedCsv = validateCsv(csvPayload)
        if (validatedCsv.trim().isEmpty()) {
            // Kein Inhalt → leeren Body zurückgeben
            message.setBody("")
            return message
        }

        /* ------------------------------------------------------------
         * 4) Mapping CSV → Liste XML-Bodies
         * ---------------------------------------------------------- */
        List<String> xmlBodies = mapCsvToXmlBodies(validatedCsv, messageLog)

        /* ------------------------------------------------------------
         * 5) POST-Call(s) an Ariba – jeder Body einzeln
         * ---------------------------------------------------------- */
        sendBodiesToAriba(xmlBodies,
                          cfg.aribaURL,
                          cfg.aribaUser,
                          cfg.aribaPwd,
                          messageLog)

        // Letztes erzeugtes XML als Body weiterreichen
        message.setBody(xmlBodies[-1])
        return message

    } catch (Exception e) {
        // Zentrales Error-Handling
        handleError(message.getBody(String) as String, e, messageLog)
        return message                     // wird nie erreicht (Exception), aber nötig für Compiler
    }
}

/* ******************************************************************
 *  Modul 1 – Header & Properties setzen
 ***************************************************************** */
private Map setHeadersAndProperties(Message message) {

    // Benötigte Property-Namen
    def propsNeeded = ['aribaURL', 'aribaUsername', 'aribaPassword',
                       'anaplanURL', 'anaplanUsername', 'anaplanPassword',
                       'SAP_Receiver', 'SAP_Sender', 'MessageID']

    propsNeeded.each { key ->
        if (!message.getProperty(key)) {
            message.setProperty(key, 'placeholder')
        }
    }

    // Header gleiches Vorgehen (bei Bedarf)
    ['SAP_Receiver', 'SAP_Sender', 'MessageID'].each { hdr ->
        if (!message.getHeader(hdr, String.class)) {
            message.setHeader(hdr, message.getProperty(hdr))
        }
    }

    // Konfiguration als Map zurückgeben
    return [
        anaplanURL : message.getProperty('anaplanURL'),
        anaplanUser: message.getProperty('anaplanUsername'),
        anaplanPwd : message.getProperty('anaplanPassword'),
        aribaURL   : message.getProperty('aribaURL'),
        aribaUser  : message.getProperty('aribaUsername'),
        aribaPwd   : message.getProperty('aribaPassword')
    ]
}

/* ******************************************************************
 *  Modul 2 – GET-Aufruf Anaplan
 ***************************************************************** */
private String callGetFromAnaplan(String urlStr,
                                  String username,
                                  String password,
                                  def messageLog) {

    HttpURLConnection conn
    try {
        URL url = new URL(urlStr)
        conn = (HttpURLConnection) url.openConnection()
        conn.with {
            requestMethod       = 'GET'
            doInput             = true
            connectTimeout      = 15000
            readTimeout         = 30000
            setRequestProperty('Authorization', "Basic ${encodeBasicAuth(username, password)}")
        }

        int rc = conn.responseCode
        if (rc != 200) {
            throw new RuntimeException("Anaplan-GET liefert HTTP ${rc}")
        }

        String body = conn.inputStream.getText('UTF-8')
        messageLog?.addAttachmentAsString("AnaplanResponse", body, "text/plain")
        return body

    } finally {
        conn?.disconnect()
    }
}

/* ******************************************************************
 *  Modul 3 – CSV-Validierung
 ***************************************************************** */
private String validateCsv(String csv) {

    if (!csv) {
        return ""
    }

    // Zeilen normalisieren
    def rows = csv.readLines().findAll { it?.trim() }

    if (rows.isEmpty()) {
        return ""
    }

    // Prüfen ob nur Header vorhanden
    if (rows.size() == 1 && rows[0].startsWith("PR_Creation_List")) {
        return ""
    }
    return rows.join("\n")
}

/* ******************************************************************
 *  Modul 4 – Mapping CSV → XML
 ***************************************************************** */
private List<String> mapCsvToXmlBodies(String csv, def messageLog) {

    // CSV-Header
    String headerLine = "PR_Creation_List,PR_PO_Status,Created_By,Request_ID," +
                        "PR_Number,Unique_Key,Request_Name,Contract_Reference," +
                        "Anaplan_Line_Number,PR_Line_Number,Quantity,Price," +
                        "Supplier_Code,SKU,Deliver_To_Code,Need_By_Date"

    def headers = headerLine.split(",")

    // CSV → Map-Liste
    def dataLines = csv.readLines()
                       .findAll { !it.startsWith("PR_Creation_List") }

    Map<String, List<Map>> grouped = [:].withDefault { [] }

    dataLines.each { line ->
        String[] cells = line.split(",", -1)
        Map row = [:]
        headers.eachWithIndex { h, idx -> row[h] = cells[idx] }

        // Status-Transformation
        String poStatus = row['PR_PO_Status'] == 'Change' ? 'Update' : row['PR_PO_Status']

        grouped[row['Request_ID']] << [
            LINE_NO        : row['Anaplan_Line_Number'],
            BUY_UNIT_PRICE : row['Price'],
            PR_PO_Status   : poStatus,
            CURRENCY       : ''                           // immer leer
        ]
    }

    /* ----------------------------------------
     * Gruppen → XML String-Liste
     * -------------------------------------- */
    List<String> xmlBodies = []

    grouped.each { reqId, lines ->
        StringWriter sw = new StringWriter()
        new MarkupBuilder(sw).PURCHASE_REQUISITION {
            REQUEST_ID(reqId)
            LINES {
                lines.each { l ->
                    PURCHASE_REQUISITION_LINE {
                        LINE_NO(l.LINE_NO)
                        BUY_UNIT_PRICE(l.BUY_UNIT_PRICE)
                        PR_PO_Status(l.PR_PO_Status)
                        CURRENCY(l.CURRENCY)
                    }
                }
            }
        }
        xmlBodies << sw.toString()
    }

    // Debug-Anhang
    messageLog?.addAttachmentAsString("MappedXML", xmlBodies.join("\n\n"), "text/xml")
    return xmlBodies
}

/* ******************************************************************
 *  Modul 5 – POST-Aufruf Ariba (pro XML-Body)
 ***************************************************************** */
private void sendBodiesToAriba(List<String> bodies,
                               String urlStr,
                               String username,
                               String password,
                               def messageLog) {

    bodies.eachWithIndex { xml, idx ->
        HttpURLConnection conn
        try {
            conn = (HttpURLConnection) new URL(urlStr).openConnection()
            conn.with {
                requestMethod  = 'POST'
                doOutput       = true
                connectTimeout = 15000
                readTimeout    = 30000
                setRequestProperty('Content-Type', 'application/xml')
                setRequestProperty('Authorization',
                                   "Basic ${encodeBasicAuth(username, password)}")
            }

            conn.outputStream.withWriter('UTF-8') { it << xml }

            int rc = conn.responseCode
            String resp = (rc >= 200 && rc < 300) ?
                          conn.inputStream.getText('UTF-8') :
                          conn.errorStream?.getText('UTF-8')

            // Antwort im Attachment hinterlegen
            messageLog?.addAttachmentAsString("AribaResponse_${idx + 1}", resp ?: "", "text/plain")

            if (rc >= 300) {
                throw new RuntimeException("Ariba-POST Fehler, HTTP ${rc}")
            }

        } finally {
            conn?.disconnect()
        }
    }
}

/* ******************************************************************
 *  Modul 6 – Basisfunktionen
 ***************************************************************** */

// Basisauth-Header codieren
private String encodeBasicAuth(String user, String pwd) {
    return Base64.getEncoder().encodeToString("${user}:${pwd}".getBytes(StandardCharsets.UTF_8))
}

/* ******************************************************************
 *  Modul 7 – Zentrales Error-Handling
 ***************************************************************** */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "", "text/plain")
    def errorMsg = "Fehler im Purchase-Request-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}