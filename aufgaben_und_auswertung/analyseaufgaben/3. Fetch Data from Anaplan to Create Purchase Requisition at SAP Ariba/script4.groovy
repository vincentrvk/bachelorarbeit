/****************************************************************************************
 *  Purchase-Request Integration Anaplan → SAP Ariba
 *  -------------------------------------------------------------------------------
 *  - Holt CSV-Daten von Anaplan
 *  - Validiert/Parst die Daten
 *  - Erstellt pro Request_ID ein XML nach Ziel-XSD
 *  - Sendet jede erzeugte XML einzeln an Ariba
 *  - Umfassendes Error-Handling mit Log-Attachments
 *
 *  Autor: AI-Assistant
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.net.HttpURLConnection
import java.net.URL

Message processData(Message message) {

    /* MessageLog für Monitoring */
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1. Header & Properties vorbereiten  */
        def props = prepareContext(message, messageLog)

        /* 2. Anaplan – CSV abrufen */
        String csvRaw = getPurchaseRequests(props, messageLog)

        /* 3. CSV-Payload validieren */
        csvRaw = validateCsv(csvRaw)

        /* Wenn leer: keinen weiteren Aufruf durchführen */
        if (csvRaw == '') {
            messageLog.addAttachmentAsString('Info', 'Kein valider Datensatz vorhanden – Verarbeitung beendet.', 'text/plain')
            message.setBody('')
            return message
        }

        /* 4. Mapping CSV → strukturierte Record-Liste  */
        List<Map> recordList = parseCsv(csvRaw)

        /* 5. Gruppierung nach Request_ID und XML-Erstellung + Versand */
        recordList.groupBy { it.REQUEST_ID }.each { reqId, lines ->
            String xmlBody = buildXml(reqId, lines)
            sendPurchaseRecord(props, reqId, xmlBody, messageLog)
        }

        /* Abschluss-Info */
        message.setBody("Erfolgreich verarbeitet: ${recordList*.REQUEST_ID.unique().join(',')}")
        return message

    } catch (Exception e) {
        /* Globale Fehlerbehandlung */
        handleError(message.getBody(String) as String, e, messageLog)
        return message     // wird nie erreicht, handleError wirft Exception
    }
}

/*-------------------------------------------------------------------------------------*/
/*  Hilfs- und Modul-Funktionen                                                         */
/*-------------------------------------------------------------------------------------*/

/* Header & Properties ermitteln bzw. Default setzen */
def prepareContext(Message message, def messageLog) {
    /* In diesem Map werden die benötigten Werte gesammelt */
    def props = [:]

    /* Relevante Properties gem. Aufgabenstellung */
    def propNames = [
            'SAP_Receiver', 'SAP_Sender', 'MessageID',
            'aribaURL', 'aribaUsername', 'aribaPassword',
            'anaplanURL', 'anaplanUsername', 'anaplanPassword'
    ]

    propNames.each { p ->
        def v = message.getProperty(p) ?: 'placeholder'
        message.setProperty(p, v)            // garantiert Vorhandensein im Message-Objekt
        props[p] = v
    }

    /* Gleiches Vorgehen für Header (falls benötigt erweiterbar) */
    return props
}

/* Ruft die CSV-Daten via GET bei Anaplan ab */
String getPurchaseRequests(Map props, def messageLog) {
    try {
        HttpURLConnection conn = (HttpURLConnection) new URL(props.anaplanURL).openConnection()
        conn.requestMethod = 'GET'
        conn.setRequestProperty('Authorization', basicAuth(props.anaplanUsername, props.anaplanPassword))

        int rc = conn.responseCode
        messageLog?.addAttachmentAsString('Anaplan-HTTP-Code', rc.toString(), 'text/plain')

        InputStream is = (rc >= 200 && rc < 300) ? conn.inputStream : conn.errorStream
        String body = is ? is.getText('UTF-8') : ''
        messageLog?.addAttachmentAsString('Anaplan-Response', body, 'text/plain')

        return body
    } catch (Exception e) {
        handleError('', e, messageLog)
    }
}

/* Prüft, ob der CSV-Payload verwertbare Daten enthält                                   */
String validateCsv(String csv) {
    if (!csv)           return ''
    def trimmed = csv.trim()
    if (trimmed == '')  return ''

    /* Header-Zeile lt. Vorgabe                        */
    def headerLine = 'PR_Creation_List,PR_PO_Status,Created_By,Request_ID,PR_Number,Unique_Key,Request_Name,Contract_Reference,Anaplan_Line_Number,PR_Line_Number,Quantity,Price,Supplier_Code,SKU,Deliver_To_Code,Need_By_Date'

    def lines = trimmed.readLines()
    if (lines.size() <= 1)                return ''
    if (lines.size() == 1 && lines[0] == headerLine) return ''

    return trimmed
}

/* Parst die CSV-Daten in eine Liste von Maps – einfache Trennung via Komma              */
List<Map> parseCsv(String csv) {
    def lines = csv.readLines()
    def header = lines[0].split(',', -1)
    List<Map> result = []

    lines.drop(1).each { l ->
        if (l?.trim()) {
            def cols = l.split(',', -1)
            def row  = [:]
            header.eachWithIndex { h, idx -> row[h.trim()] = cols.size() > idx ? cols[idx].trim() : '' }
            /* Spalten für Mapping umbenennen / bereinigen */
            result << [
                    REQUEST_ID           : row['Request_ID'],
                    LINE_NO              : row['Anaplan_Line_Number'],
                    BUY_UNIT_PRICE       : row['Price'],
                    PR_PO_Status         : (row['PR_PO_Status'] == 'Change' ? 'Update' : row['PR_PO_Status']),
                    CURRENCY             : ''                           // gem. Vorgabe leer
            ]
        }
    }
    return result
}

/* Baut das Ziel-XML für eine Request_ID                                                */
String buildXml(String requestId, List<Map> lines) {
    StringWriter sw = new StringWriter()
    new MarkupBuilder(sw).PURCHASE_REQUISITION {
        REQUEST_ID(requestId)
        LINES {
            lines.each { ln ->
                PURCHASE_REQUISITION_LINE {
                    LINE_NO(ln.LINE_NO)
                    BUY_UNIT_PRICE(ln.BUY_UNIT_PRICE)
                    PR_PO_Status(ln.PR_PO_Status)
                    CURRENCY(ln.CURRENCY)
                }
            }
        }
    }
    return sw.toString()
}

/* Sendet ein einzelnes Purchase-Request-XML an Ariba                                   */
void sendPurchaseRecord(Map props, String reqId, String xmlBody, def messageLog) {
    try {
        HttpURLConnection conn = (HttpURLConnection) new URL(props.aribaURL).openConnection()
        conn.requestMethod = 'POST'
        conn.doOutput      = true
        conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
        conn.setRequestProperty('Authorization', basicAuth(props.aribaUsername, props.aribaPassword))

        conn.outputStream.withWriter('UTF-8') { it << xmlBody }

        int rc = conn.responseCode
        String resp = (rc >= 200 && rc < 300) ? conn.inputStream?.getText('UTF-8')
                                              : conn.errorStream?.getText('UTF-8')

        messageLog?.addAttachmentAsString("Ariba-Response-${reqId}", "HTTP ${rc}\n${resp}", 'text/plain')
    } catch (Exception e) {
        handleError(xmlBody, e, messageLog)
    }
}

/* Erstellt das Basic-Auth-Header-Value */
String basicAuth(String usr, String pwd) {
    return 'Basic ' + "${usr}:${pwd}".bytes.encodeBase64().toString()
}

/* Zentrale Fehlerbehandlung – wirft RuntimeException                                   */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}