/******************************************************************************
*  Skriptname  :  ImportPurchaseRequestAnaplan2Ariba.groovy
*  Beschreibung:  Liest Purchase-Request-CSV aus Anaplan, validiert, wandelt in
*                 XML um, sendet je Request_ID eine POST-Anfrage an Ariba.
*  Autor       :  Groovy-Beispiel – Senior Integration Developer
******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.net.HttpURLConnection
import java.net.URL

/*==========================================================================
  Öffentliche Hauptroutine
  ------------------------------------------------------------------------*/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    try {

        /*--------------------------------------------------------------
          1. Properties / Header auslesen oder mit 'placeholder' setzen
        --------------------------------------------------------------*/
        def cfg = resolveConfigValues(message)

        /*--------------------------------------------------------------
          2. Anaplan CSV abholen
        --------------------------------------------------------------*/
        String csvPayload = fetchAnaplanCsv(cfg, messageLog)

        /*--------------------------------------------------------------
          3. Payload validieren
        --------------------------------------------------------------*/
        if (isEmptyCsv(csvPayload)) {
            message.setBody('')
            messageLog?.addAttachmentAsString('Info', 'Kein zu importierender Datensatz vorhanden.', 'text/plain')
            return message
        }

        /*--------------------------------------------------------------
          4. CSV parsen & gruppieren
        --------------------------------------------------------------*/
        Map<String, List<Map<String, String>>> groupedRecords = parseCsv(csvPayload)

        /*--------------------------------------------------------------
          5. Mapping -> XML & Senden an Ariba
        --------------------------------------------------------------*/
        int counter = 0
        groupedRecords.each { requestId, rows ->
            counter++
            String xmlBody = buildXml(requestId, rows)
            String aribaResp = sendToAriba(xmlBody, cfg, counter, messageLog)
            messageLog?.addAttachmentAsString("AribaResponse_${counter}", aribaResp ?: '', 'text/plain')
        }

        /*--------------------------------------------------------------
          6. Erfolgsmeldung zurückgeben
        --------------------------------------------------------------*/
        message.setBody("Es wurden ${groupedRecords.size()} Purchase-Requests an Ariba übertragen.")
        return message

    } catch (Exception e) {
        /*----------------------------------------------------------
          Zentrales Error-Handling
        ----------------------------------------------------------*/
        handleError(message.getBody(String) ?: '', e, messageLog)
        return message     // wird durch throw im handleError nie erreicht
    }
}

/*==========================================================================
  Hilfs- & Servicefunktionen
  ------------------------------------------------------------------------*/

/*------------------------------------------------------------------------
  Liest Properties/Headers oder setzt 'placeholder'
------------------------------------------------------------------------*/
private Map resolveConfigValues(Message message) {

    def getter = { String key ->
        message.getProperty(key) ?:
        message.getHeader(key, String) ?:
        'placeholder'
    }

    return [
            SAP_Receiver    : getter('SAP_Receiver'),
            SAP_Sender      : getter('SAP_Sender'),
            MessageID       : getter('MessageID'),
            aribaURL        : getter('aribaURL'),
            aribaUsername   : getter('aribaUsername'),
            aribaPassword   : getter('aribaPassword'),
            anaplanURL      : getter('anaplanURL'),
            anaplanUsername : getter('anaplanUsername'),
            anaplanPassword : getter('anaplanPassword')
    ]
}

/*------------------------------------------------------------------------
  GET-Call an Anaplan – CSV zurückliefern
------------------------------------------------------------------------*/
private String fetchAnaplanCsv(Map cfg, def messageLog) {

    try {
        HttpURLConnection conn = (HttpURLConnection) new URL(cfg.anaplanURL).openConnection()
        conn.requestMethod = 'GET'
        conn.setRequestProperty('Authorization', buildBasicAuth(cfg.anaplanUsername, cfg.anaplanPassword))
        conn.connect()

        int rc = conn.responseCode
        messageLog?.addAttachmentAsString('Anaplan_HTTP_RC', String.valueOf(rc), 'text/plain')

        if (rc == 200) {
            return conn.inputStream.getText('UTF-8')
        } else {
            String err = conn.errorStream?.getText('UTF-8') ?: ''
            throw new RuntimeException("Anaplan-Aufruf fehlgeschlagen (HTTP ${rc}). ${err}")
        }
    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Anaplan-Aufruf: ${e.message}", e)
    }
}

/*------------------------------------------------------------------------
  Prüft ob CSV leer, nur Klammern oder nur Header enthält
------------------------------------------------------------------------*/
private boolean isEmptyCsv(String csv) {

    if (!csv)                       { return true }
    if (csv.trim() in ['()', '[]']) { return true }

    def lines = csv.readLines().findAll { it?.trim() }
    if (lines.size() <= 1)          { return true }   // nur Header

    return false
}

/*------------------------------------------------------------------------
  CSV in Map gruppiert nach Request_ID umwandeln
------------------------------------------------------------------------*/
private Map<String, List<Map<String, String>>> parseCsv(String csv) {

    List<String> cols = [
            'PR_Creation_List','PR_PO_Status','Created_By','Request_ID','PR_Number',
            'Unique_Key','Request_Name','Contract_Reference','Anaplan_Line_Number',
            'PR_Line_Number','Quantity','Price','Supplier_Code','SKU',
            'Deliver_To_Code','Need_By_Date'
    ]

    Map<String, List<Map<String, String>>> grouped = [:].withDefault { [] }

    csv.readLines().drop(1).each { line ->
        List<String> values = line.split(',', -1).collect { it?.trim() }
        if (values.size() == cols.size()) {
            def row = [ : ]
            cols.eachWithIndex { c, idx -> row[c] = values[idx] }
            grouped[row.Request_ID] << row
        }
    }
    return grouped
}

/*------------------------------------------------------------------------
  Mapping & XML-Erstellung für eine Request_ID
------------------------------------------------------------------------*/
private String buildXml(String requestId, List<Map<String, String>> rows) {

    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)

    xml.PURCHASE_REQUISITION {
        REQUEST_ID(requestId)
        LINES {
            rows.each { r ->
                PURCHASE_REQUISITION_LINE {
                    LINE_NO              (r.Anaplan_Line_Number)
                    BUY_UNIT_PRICE       (r.Price)
                    PR_PO_Status         (r.PR_PO_Status == 'Change' ? 'Update' : r.PR_PO_Status)
                    CURRENCY             ('')
                }
            }
        }
    }
    return sw.toString()
}

/*------------------------------------------------------------------------
  POST-Call an Ariba für einen XML-Body
------------------------------------------------------------------------*/
private String sendToAriba(String xmlBody, Map cfg, int counter, def messageLog) {

    try {
        HttpURLConnection conn = (HttpURLConnection) new URL(cfg.aribaURL).openConnection()
        conn.requestMethod = 'POST'
        conn.doOutput = true
        conn.setRequestProperty('Authorization', buildBasicAuth(cfg.aribaUsername, cfg.aribaPassword))
        conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')

        conn.outputStream.withWriter('UTF-8') { it << xmlBody }
        conn.connect()

        int rc = conn.responseCode
        messageLog?.addAttachmentAsString("Ariba_HTTP_RC_${counter}", String.valueOf(rc), 'text/plain')

        if (rc in 200..299) {
            return conn.inputStream?.getText('UTF-8')
        } else {
            String err = conn.errorStream?.getText('UTF-8') ?: ''
            throw new RuntimeException("Ariba-Aufruf (Request ${counter}) schlug fehl (HTTP ${rc}). ${err}")
        }

    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Ariba-Aufruf (Request ${counter}): ${e.message}", e)
    }
}

/*------------------------------------------------------------------------
  Erstellt Basic-Auth-Header
------------------------------------------------------------------------*/
private String buildBasicAuth(String user, String pwd) {
    return 'Basic ' + "${user}:${pwd}".bytes.encodeBase64().toString()
}

/*------------------------------------------------------------------------
  Zentrales Error-Handling  (Payload als Attachment, Exception re-throw)
------------------------------------------------------------------------*/
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}