/********************************************************************************************
 * Groovy-Skript:   Import Employee Benefits in SAP CPI
 * Beschreibung:   Validiert eingehende Pipe-getrennte CSV-Daten, transformiert valide
 *                 Datensätze in XML gem. Ziel-XSD und sendet jeden Datensatz einzeln
 *                 per HTTP-POST an SuccessFactors.  Invalide Records werden als CSV
 *                 (PersonID|Validation) im Message-Body zurückgegeben.
 * Autor:          ChatGPT-Senior-Dev
 *******************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.text.SimpleDateFormat
import java.net.HttpURLConnection

/* ========================================================================= */
/* Main                                                                      */
/* ========================================================================= */
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    def originalBody = message.getBody(String) ?: ''

    try {
        /* 1. Konfigurationswerte laden (Properties / Header / Platzhalter) */
        def cfg = readConfiguration(message)

        /* 2. CSV verarbeiten */
        def (validRecords, invalidRecords) = validateCsv(originalBody, messageLog)

        /* 3. Valide Datensätze an SF senden */
        validRecords.each { rec ->
            def xmlPayload = mapRecordToXml(rec)
            sendRecordToSF(xmlPayload, cfg, rec.PayrollID, messageLog)
        }

        /* 4. Invalide Records als CSV im Body zurückliefern */
        String invalidCsv = buildInvalidCsv(invalidRecords)
        message.setBody(invalidCsv ?: 'ALL_RECORDS_VALID')

    } catch (Exception e) {
        handleError(originalBody, e, messageLog)   // Wird nie zurückgegeben
    }
    return message
}

/* ========================================================================= */
/* Modul 1 –  Konfiguration                                                  */
/* ========================================================================= */
/* Liest Username, Password und URL aus Properties / Headern oder setzt      */
/* "placeholder", falls leer.                                                */
private static Map readConfiguration(Message msg) {
    def getVal = { String key ->
        def val = msg.getProperty(key) ?: msg.getHeader(key, String.class)
        return (val ? val.toString() : 'placeholder')
    }
    [
            username : getVal('successFactorsUsername'),
            password : getVal('successFactorsPassword'),
            url      : getVal('successFactorsURL')
    ]
}

/* ========================================================================= */
/* Modul 2 –  Validierung                                                    */
/* ========================================================================= */
/* Prüft alle Validierungsregeln und liefert zwei Listen zurück:             */
/*  (1) valide Records (List<Map>)                                           */
/*  (2) invalide Records (List<Map> mit Feld 'reason')                       */
private static List validateCsv(String csvBody, def msgLog) {
    List<Map> valid   = []
    List<Map> invalid = []

    if (!csvBody?.trim()) {
        throw new RuntimeException('Leerer CSV-Payload empfangen.')
    }

    def lines = csvBody.split(/\r?\n/).toList()
    if (lines.size() < 2) {
        throw new RuntimeException('CSV enthält keine Datenzeilen.')
    }

    def headers = lines[0].split(/\|/) as List<String>

    lines.tail().eachWithIndex { String line, int idx ->
        if (!line?.trim()) { return } // leere Zeile ignorieren
        def cols = line.split(/\|/, -1)   // -1  -> leere Felder behalten
        def rec  = [:]
        headers.eachWithIndex { h, i -> rec[h.trim()] = (i < cols.size() ? cols[i].trim() : '') }

        List<String> reasons = []

        // -------------- Regelprüfungen -----------------------------------
        if (!rec.ValidityPeriod_StartDate) reasons << 'Missing Start Date'
        if (!rec.ValidityPeriod_EndDate)   reasons << 'Missing End Date'
        if (!rec.Amount)                   reasons << 'Missing Amount'
        if (rec.Record_Type?.equalsIgnoreCase('E')) reasons << 'RecordType is E'

        String payrollId = rec.PayrollID ?: ''
        if (!payrollId)                    reasons << 'Missing PayrollID'
        if (payrollId.size() >= 9)         reasons << 'PayrollID too long'
        if (!(payrollId ==~ /^\d+$/))      reasons << 'PayrollID not numeric'

        // -----------------------------------------------------------------
        if (reasons) {
            rec.reason = reasons.unique().join(', ')
            invalid << rec
            msgLog?.addAttachmentAsString("Invalid_Record_Line_${idx+2}", line, 'text/plain')
        } else {
            valid << rec
        }
    }
    return [valid, invalid]
}

/* ========================================================================= */
/* Modul 3 –  Mapping                                                        */
/* ========================================================================= */
/* Erstellt XML-String (nur <EmployeeBenefitsVendorData/>) für einen Record  */
private static String mapRecordToXml(Map rec) {
    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)

    xml.EmployeeBenefitsVendorData {
        EmployeeID(rec.PayrollID)
        EmployeeBenefitData {
            ValidityPeriod {
                StartDate(formatDate(rec.ValidityPeriod_StartDate))
                EndDate  (formatDate(rec.ValidityPeriod_EndDate))
            }
            CompensationComponentTypeID(rec.CompenstationComponentTypeID)
            Amount(rec.Amount) {
                mkp.attribute 'currencyCode', 'USD'
            }
            GoalAmount(rec.GoalAmount) {
                mkp.attribute 'currencyCode', 'USD'
            }
            PercentageContribution(cleanPercentage(rec.PercentageContribution))
        }
    }
    return sw.toString()
}

/* Hilfsfunktion – Datum normalisieren (yyyy-MM-dd) */
private static String formatDate(String inDate) {
    if (!inDate) return ''
    def formats = ['yyyy-MM-dd', 'yyyyMMdd']
    Date dt = null
    formats.find { fmt ->
        try {
            dt = new SimpleDateFormat(fmt).parse(inDate)
            return true
        } catch (Exception ignored) { return false }
    }
    if (!dt) throw new RuntimeException("Ungültiges Datumsformat: $inDate")
    return new SimpleDateFormat('yyyy-MM-dd').format(dt)
}

/* Hilfsfunktion – Prozentzeichen entfernen                                  */
private static String cleanPercentage(String pct) {
    return pct?.replace('%', '') ?: ''
}

/* ========================================================================= */
/* Modul 4 –  API-Call zu SuccessFactors                                     */
/* ========================================================================= */
/* Sendet einen XML-Datensatz per POST; Loggt Responsecode und Fehler        */
private static void sendRecordToSF(String xml, Map cfg,
                                   String payrollId, def msgLog) {
    HttpURLConnection conn = null
    try {
        conn = (HttpURLConnection) new URL(cfg.url).openConnection()
        conn.with {
            requestMethod = 'POST'
            doOutput      = true
            connectTimeout = 30000
            readTimeout    = 30000
            setRequestProperty('Content-Type', 'application/xml;charset=UTF-8')
            def auth = "${cfg.username}:${cfg.password}".bytes.encodeBase64().toString()
            setRequestProperty('Authorization', "Basic ${auth}")
        }

        conn.outputStream.withWriter('UTF-8') { it << xml }

        int rc = conn.responseCode
        if (rc >= 200 && rc < 300) {
            msgLog?.addAttachmentAsString("SF_OK_${payrollId}", "HTTP ${rc}", 'text/plain')
        } else {
            String err = conn.errorStream?.getText('UTF-8') ?: ''
            msgLog?.addAttachmentAsString("SF_ERR_${payrollId}", "HTTP ${rc}\n${err}", 'text/plain')
        }
    } finally {
        conn?.disconnect()
    }
}

/* ========================================================================= */
/* Modul 5 –  Invalid-CSV erzeugen                                           */
/* ========================================================================= */
private static String buildInvalidCsv(List<Map> invalidRecords) {
    if (!invalidRecords) return ''
    def sb = new StringBuilder()
    sb.append('PersonID|Validation\n')
    invalidRecords.each { rec ->
        sb.append("${rec.PayrollID}|${rec.reason}\n")
    }
    return sb.toString().trim()
}

/* ========================================================================= */
/* Modul 6 –  Error-Handling                                                 */
/* ========================================================================= */
/* Fügt den Original-Payload als Attachment an und wirft RuntimeException    */
private static void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    def errMsg = "Fehler im Employee-Import-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}