/*******************************************************************************
 *  SAP Cloud Integration – Groovy-Skript
 *
 *  Aufgabe:
 *  1. Einlesen einer Pipe-getrennten CSV-Datei
 *  2. Validierung jedes Datensatzes gem. Vorgaben
 *  3. Transformation valider Datensätze in XML (EmployeeBenefitsVendorData)
 *  4. Einzelner POST-Aufruf je validem Record an SuccessFactors
 *  5. Rückgabe aller invalider Datensätze als CSV (PersonID|Validation)
 *
 *  Hinweis:
 *  • Alle Funktionen sind modular aufgebaut (siehe unten)
 *  • Jede Funktion enthält deutschsprachige Kommentare
 *  • Fehlerbehandlung zentral über handleError()
 ******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.text.SimpleDateFormat

Message processData(Message message) {

    // ---------- Initialisierung ------------------------------------------------
    def messageLog = messageLogFactory?.getMessageLog(message)
    String originalBody = message.getBody(String) ?: ''

    try {

        /* 1. Variablen (Header/Properties) ermitteln */
        Map<String, String> vars = fetchVariables(message)

        /* 2. CSV parsen und validieren */
        List<Map> validRecords   = []
        List<Map> invalidRecords = []

        parseCsv(originalBody).each { Map rec ->
            List<String> errors = validateRecord(rec)
            if (errors.isEmpty()) {
                validRecords << rec
            } else {
                rec.put('__validation__', errors.join('; '))
                invalidRecords << rec
            }
        }

        /* 3. Valide Records -> XML und POST */
        validRecords.each { Map rec ->
            String xmlPayload = mapRecordToXml(rec)
            sendRecord(xmlPayload, vars, messageLog)
        }

        /* 4. Invalide Records als CSV zurückgeben */
        message.setBody(buildInvalidCsv(invalidRecords))

    } catch (Exception e) {
        /* zentrales Error-Handling */
        handleError(originalBody, e, messageLog)
    }

    return message
}

/* ------------------------------------------------------------------------- */
/* -----------------------------  FUNCTIONS  ------------------------------- */
/* ------------------------------------------------------------------------- */

/* Variablen (Header/Properties) lesen/setzen */
private Map<String,String> fetchVariables(Message message) {
    /* Jedes Property/Header auf Existenz prüfen, sonst Placeholder setzen */
    String user = (message.getProperty('successFactorsUsername') ?: 
                   message.getHeader('successFactorsUsername', String.class) ?: 'placeholder')
    String pwd  = (message.getProperty('successFactorsPassword') ?: 
                   message.getHeader('successFactorsPassword', String.class) ?: 'placeholder')
    String url  = (message.getProperty('successFactorsURL') ?: 
                   message.getHeader('successFactorsURL', String.class) ?: 'placeholder')

    return [USER:user, PWD:pwd, URL:url]
}

/* CSV einlesen – Rückgabe: Liste von Map pro Zeile */
private List<Map> parseCsv(String csvText) {
    List<Map> rows = []
    if (!csvText?.trim()) { return rows }

    def lines = csvText.split(/\r?\n/).findAll { it.trim() }
    String[] headers = lines[0].split(/\|/)

    lines.drop(1).each { String line ->
        String[] values = line.split(/\|/, -1)                       // -1 => leere Felder behalten
        Map row = [:]
        headers.eachWithIndex { h, idx -> row[h.trim()] = values[idx]?.trim() }
        rows << row
    }
    return rows
}

/* Validierung eines Records – Rückgabe: Liste Fehlertexte */
private List<String> validateRecord(Map rec) {
    List<String> errors = []

    if (!rec.ValidityPeriod_StartDate)                errors << 'Missing Start Date'
    if (!rec.ValidityPeriod_EndDate)                  errors << 'Missing End Date'
    if (!rec.Amount)                                  errors << 'Missing Amount'
    if ((rec.PayrollID ?: '').length() >= 9)          errors << 'PayrollID too long'
    if (!rec.PayrollID?.isInteger())                  errors << 'PayrollID not numeric'
    if (rec.Record_Type?.equalsIgnoreCase('E'))       errors << 'RecordType = E (invalid)'

    return errors
}

/* Mapping Record -> EmployeeBenefitsVendorData (XML) */
private String mapRecordToXml(Map rec) {

    /* Hilfsfunktionen für Datum/Konvertierungen */
    String normalizeDate(String d) {
        if (!d) return ''
        // unterstützt Formate yyyyMMdd oder yyyy-MM-dd
        if (d ==~ /\d{8}/) {
            return d[0..3]+'-'+d[4..5]+'-'+d[6..7]
        } else if (d ==~ /\d{4}-\d{2}-\d{2}/) {
            return d
        } else {
            return d // unbekanntes Format – unbearbeitet zurück
        }
    }
    String employeeId = rec.PayrollID?.replaceAll(/\D/,'') ?: ''

    StringWriter sw = new StringWriter()
    MarkupBuilder xml = new MarkupBuilder(sw)
    xml.EmployeeBenefitsVendorData {
        EmployeeID(employeeId)
        EmployeeBenefitData {
            ValidityPeriod {
                StartDate(normalizeDate(rec.ValidityPeriod_StartDate))
                EndDate  (normalizeDate(rec.ValidityPeriod_EndDate))
            }
            CompensationComponentTypeID(rec.CompenstationComponentTypeID)
            Amount(rec.Amount, currencyCode:'USD')
            GoalAmount(rec.GoalAmount ?: rec.Amount, currencyCode:'USD')
            PercentageContribution((rec.PercentageContribution ?: '0').replace('%',''))
        }
    }
    return sw.toString()
}

/* POST-Aufruf an SuccessFactors */
private void sendRecord(String xmlPayload, Map vars, def messageLog) {

    URL url = new URL(vars.URL)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Content-Type', 'application/xml')
    String auth = "${vars.USER}:${vars.PWD}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', 'Basic ' + auth)

    // Body schreiben
    conn.outputStream.withWriter('UTF-8') { it << xmlPayload }

    int rc = conn.responseCode
    if (rc != 200 && rc != 201) {
        String respMsg = conn.errorStream?.getText('UTF-8') ?: conn.inputStream?.getText('UTF-8')
        throw new RuntimeException("HTTP Fehler ${rc}: ${respMsg}")
    }

    /* Logging */
    messageLog?.addAttachmentAsString("SF_ECP_Response_${System.currentTimeMillis()}",
                                      conn.inputStream?.getText('UTF-8') ?: '', 'text/plain')
}

/* CSV für invalide Records erzeugen */
private String buildInvalidCsv(List<Map> invalidRecords) {
    if (!invalidRecords) return ''

    StringBuilder sb = new StringBuilder('PersonID|Validation\n')
    invalidRecords.each { Map rec ->
        sb.append("${rec.PayrollID ?: ''}|${rec.__validation__}\n")
    }
    return sb.toString().trim()
}

/* ---------- Zentrales Error-Handling ------------------------------------- */
def handleError(String body, Exception e, def messageLog) {
    /* Payload als Attachment anfügen und Exception werfen */
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

/* ---------- Hilfsmethode -------------------------------------------------- */
/* Prüft, ob String nur aus Ziffern besteht – erspart Import von Commons-Lang */
private Boolean String.isInteger() { return this ==~ /\d+/ }