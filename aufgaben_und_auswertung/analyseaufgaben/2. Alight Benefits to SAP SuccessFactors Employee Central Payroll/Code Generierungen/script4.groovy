import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.text.SimpleDateFormat

Message processData(Message message) {
    //--- Lokale Hilfsvariablen ------------------------------------------------
    def messageLog = messageLogFactory.getMessageLog(message)
    def originalBody = message.getBody(String) ?: ''

    try {
        /* 1) Header- & Property-Handling */
        def config = resolveTechnicalValues(message)

        /* 2) CSV parsen und validieren */
        def (validRecords, invalidRecords) = parseAndValidateCsv(originalBody, messageLog)

        /* 3) Verarbeitung valider Records */
        validRecords.each { rec ->
            def xmlFragment = mapRecordToXml(rec)
            sendRecord(xmlFragment, config, messageLog)
        }

        /* 4) Umgang mit invaliden Records                                         
              – falls vorhanden als CSV zurückgeben                              */
        if (invalidRecords) {
            message.setBody(buildInvalidCsv(invalidRecords))
            message.setHeader('Content-Type', 'text/csv')
        } else {
            // Kein spezieller Rückgabewert gefordert – leeren Body setzen
            message.setBody('')
        }

        return message
    } catch (Exception e) {
        // zentrales Fehler-Handling
        handleError(originalBody, e, messageLog)
        return message        // wird von handleError nie erreicht (RuntimeException)
    }
}

/* ======================================================================== */
/* ==============================  MODULES  =============================== */
/* ======================================================================== */

/* Technische Werte (URL, User, PW) ermitteln                               */
private Map resolveTechnicalValues(Message msg) {
    // Holt Property, fällt zurück auf Header, dann Platzhalter
    def fetch = { String key ->
        msg.getProperty(key) ?: msg.getHeader(key, String) ?: 'placeholder'
    }
    return [
        url     : fetch('successFactorsURL'),
        user    : fetch('successFactorsUsername'),
        pwd     : fetch('successFactorsPassword')
    ]
}

/* CSV einlesen, Validierung durchführen                                    */
private List parseAndValidateCsv(String body, def msgLog) {
    def valid   = []
    def invalid = []

    // Leere oder fehlende Payload?
    if (!body?.trim()) {
        throw new IllegalArgumentException('Leere Payload erhalten.')
    }

    def lines = body.trim().split(/\r?\n/)
    def header = lines[0].split(/\|/)*.trim()
    def rows   = lines.drop(1)

    rows.eachWithIndex { line, idx ->
        def values = line.split(/\|/, -1)*.trim()
        def rec    = header.collectEntries { h -> [ (h) : '' ] }   // Default-Map
        header.eachWithIndex { h, id -> rec[h] = id < values.size() ? values[id] : '' }

        def validationResult = validateRecord(rec)
        if (validationResult.valid) {
            valid << rec
        } else {
            invalid << [
                PayrollID : rec.PayrollID ?: '',
                reason    : validationResult.reason.join(', ')
            ]
            msgLog?.addAttachmentAsString("Invalid_Record_${idx+1}", line, 'text/plain')
        }
    }
    return [valid, invalid]
}

/* Record-Validierung                                                       */
private Map validateRecord(Map rec) {
    def reasons = []

    // Pflichtfelder
    ['ValidityPeriod_StartDate', 'ValidityPeriod_EndDate', 'PayrollID', 'Amount'].each {
        if (!rec[it]) reasons << "Missing ${it.replace('ValidityPeriod_', '').replace('_', ' ')}"
    }

    // PayrollID Prüfung
    if (rec.PayrollID && rec.PayrollID.length() >= 9) reasons << 'PayrollID too long'
    if (rec.Record_Type == 'E')                        reasons << 'RecordType is E'
    if (rec.PayrollID && !rec.PayrollID.isInteger())   reasons << 'PayrollID is not numeric'

    return [valid: reasons.empty, reason: reasons]
}

/* Mapping: CSV-Record ➜ XML-Fragment                                       */
private String mapRecordToXml(Map rec) {
    // Datums­formate anpassen
    def inFmt  = new SimpleDateFormat(rec.ValidityPeriod_StartDate.contains('-') ? 'yyyy-MM-dd' : 'yyyyMMdd')
    def outFmt = new SimpleDateFormat('yyyy-MM-dd')

    def sw = new StringWriter()
    new MarkupBuilder(sw).'EmployeeBenefitsVendorData' {
        'EmployeeID'(rec.PayrollID.replaceAll('[^0-9]', ''))
        'EmployeeBenefitData' {
            'ValidityPeriod' {
                'StartDate'(outFmt.format(inFmt.parse(rec.ValidityPeriod_StartDate)))
                'EndDate'  (outFmt.format(inFmt.parse(rec.ValidityPeriod_EndDate)))
            }
            'CompensationComponentTypeID'(rec.CompenstationComponentTypeID ?: '')
            'Amount'(rec.Amount) { mkp.attribute('currencyCode', 'USD') }
            'GoalAmount'(rec.GoalAmount ?: '') { mkp.attribute('currencyCode', 'USD') }
            'PercentageContribution'(rec.PercentageContribution?.replace('%','') ?: '')
        }
    }
    return sw.toString()
}

/* API-Aufruf                                                               */
private void sendRecord(String xmlFragment, Map cfg, def msgLog) {
    def fullXml = """<?xml version="1.0" encoding="UTF-8"?>
<EmployeeBenefitsVendorData_List>${xmlFragment}</EmployeeBenefitsVendorData_List>"""

    def urlConn = new URL(cfg.url).openConnection()
    urlConn.with {
        doOutput       = true
        requestMethod  = 'POST'
        setRequestProperty('Authorization', 'Basic ' +
                "${cfg.user}:${cfg.pwd}".bytes.encodeBase64().toString())
        setRequestProperty('Content-Type', 'application/xml')
        outputStream.withWriter('UTF-8') { it << fullXml }
    }

    int rc = urlConn.responseCode
    if (rc >= 200 && rc < 300) {
        msgLog?.addAttachmentAsString("SF_Response_${System.currentTimeMillis()}", "HTTP ${rc}", 'text/plain')
    } else {
        throw new RuntimeException("Fehler beim Senden an SuccessFactors, HTTP-Code: ${rc}")
    }
}

/* CSV-Builder für invalide Datensätze                                      */
private String buildInvalidCsv(List invalidRecords) {
    def lines = ['PersonID|Validation']
    invalidRecords.each { lines << "${it.PayrollID}|${it.reason}" }
    return lines.join('\n')
}

/* Zentrales Fehler-Handling                                                */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/plain')
    def msg = "Fehler im Import-Skript: ${e.message}"
    throw new RuntimeException(msg, e)
}

/* =======================  UTILITY-METHODEN ============================== */
private static boolean String.isInteger() {
    return this.isNumber() && !this.contains('.')
}