/*
*  Groovy-Skript: Import von Mitarbeitern nach SAP SuccessFactors ECP
*  Autor: Senior-Integration-Entwickler
*  Hinweis: Alle Funktionen sind bewusst modular aufgebaut – siehe Aufgabenstellung.
*/
import com.sap.gateway.ip.core.customdev.util.Message
import java.text.SimpleDateFormat
import groovy.xml.MarkupBuilder
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.Base64

/* =================================================================================
 *  HAUPTPROZESS
 * ================================================================================= */
Message processData(Message message) {

    final String originalBody = message.getBody(String) ?: ''
    def messageLog          = messageLogFactory.getMessageLog(message)

    try {

        /* 1. Kontextwerte ermitteln (Properties & Header) */
        def ctx = readContextValues(message)

        /* 2. CSV parsen */
        List<Map<String,String>> records = parseCsv(originalBody)

        /* 3. Records verarbeiten */
        List<Map<String,String>> invalidRecords = []

        records.each { rec ->
            List<String> valErrors = validateRecord(rec)

            if (valErrors.isEmpty()) {
                /* 3a. Mapping zu XML */
                String recordXml = buildXmlRecord(rec)

                /* 3b. Senden an SuccessFactors */
                sendRecordToSFSF(recordXml, ctx, messageLog)
            } else {
                /* 3c. Sammeln invalider Records */
                rec.put('Validation', valErrors.join('; '))
                invalidRecords << rec
            }
        }

        /* 4. Invalide Records als CSV zurückgeben */
        String invalidCsv = buildInvalidCsv(invalidRecords)
        message.setBody(invalidCsv)

    } catch (Exception e) {
        /* 5. zentrales Error-Handling */
        handleError(originalBody, e, messageLog)
    }

    return message
}

/* =================================================================================
 *  FUNKTION: Kontextwerte (Header & Properties) einlesen
 * ================================================================================= */
private Map<String,String> readContextValues(Message msg) {
    /*
     * Liest Username, Passwort & URL aus Message-Properties / Headern.
     * Existiert kein Wert, wird 'placeholder' verwendet.
     */
    String user = (msg.getProperty('successFactorsUsername')
                ?: msg.getHeader('successFactorsUsername', String.class)) ?: 'placeholder'

    String pwd  = (msg.getProperty('successFactorsPassword')
                ?: msg.getHeader('successFactorsPassword', String.class)) ?: 'placeholder'

    String url  = (msg.getProperty('successFactorsURL')
                ?: msg.getHeader('successFactorsURL', String.class)) ?: 'placeholder'

    return [username:user, password:pwd, targetUrl:url]
}

/* =================================================================================
 *  FUNKTION: CSV in Record-Maps umwandeln
 * ================================================================================= */
private List<Map<String,String>> parseCsv(String csvText) {
    /*
     * Erwartet Pipe-getrennte CSV-Daten. Erste Zeile = Header. Wandelt jede Zeile
     * in ein Map<String,String> um.
     */
    if (!csvText?.trim()) { return [] }

    List<String> lines     = csvText.split(/\r?\n/).toList()
    List<String> headers   = lines.head().split(/\|/).collect{ it.trim() }

    lines.tail().collect { line ->
        List<String> values = line.split(/\|/, -1).collect{ it.trim() }
        headers.collectEntries{ hdr -> [ (hdr) : values[headers.indexOf(hdr)] ?: '' ] }
    }
}

/* =================================================================================
 *  FUNKTION: Record-Validierung
 * ================================================================================= */
private List<String> validateRecord(Map<String,String> rec) {
    /*
     * Gibt eine Liste aller gefundenen Validierungsfehler zurück.
     */
    List<String> errors = []

    if (!rec.ValidityPeriod_StartDate)      { errors << 'Missing Start Date' }
    if (!rec.ValidityPeriod_EndDate)        { errors << 'Missing End Date' }
    if (!rec.Amount)                        { errors << 'Missing Amount'   }

    /* PayrollID muss numerisch & < 9 Stellen sein */
    if (!rec.PayrollID?.isNumber())         { errors << 'PayrollID not numeric' }
    else if (rec.PayrollID.size() >= 9)     { errors << 'PayrollID too long' }

    /* RecordType darf nicht "E" sein */
    if (rec.Record_Type?.equalsIgnoreCase('E')) { errors << 'RecordType = E' }

    return errors
}

/* =================================================================================
 *  FUNKTION: Mapping zu XML (einzelner Record)
 * ================================================================================= */
private String buildXmlRecord(Map<String,String> rec) {

    /* Datumsformat-Konvertierung */
    String start = convertDate(rec.ValidityPeriod_StartDate)
    String end   = convertDate(rec.ValidityPeriod_EndDate)

    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)

    xml.EmployeeBenefitsVendorData_List {
        EmployeeBenefitsVendorData {
            EmployeeID   ( rec.PayrollID.toInteger() )
            EmployeeBenefitData {
                ValidityPeriod {
                    StartDate ( start )
                    EndDate   ( end )
                }
                CompensationComponentTypeID ( rec.CompenstationComponentTypeID )
                Amount ( rec.Amount ) { mkp.attribute('currencyCode','USD') }
                if (rec.GoalAmount) {
                    GoalAmount ( rec.GoalAmount ) {
                        mkp.attribute('currencyCode','USD')
                    }
                }
                if (rec.PercentageContribution) {
                    PercentageContribution ( rec.PercentageContribution.replace('%','') )
                }
            }
        }
    }
    return sw.toString()
}

/* =================================================================================
 *  FUNKTION: HTTP-Aufruf an SuccessFactors
 * ================================================================================= */
private void sendRecordToSFSF(String xmlBody, Map ctx, def messageLog) {
    /*
     * Führt einen synchronen POST Request mit Basic Auth aus.
     * Bei Fehlern wird eine RuntimeException geworfen.
     */
    URL url = new URL(ctx.targetUrl)
    def conn = (HttpURLConnection) url.openConnection()
    conn.with {
        requestMethod = 'POST'
        doOutput      = true
        setRequestProperty('Authorization', 'Basic ' +
                Base64.encoder.encodeToString("${ctx.username}:${ctx.password}".getBytes(StandardCharsets.UTF_8)))
        outputStream.withWriter('UTF-8') { it << xmlBody }
    }

    int rc = conn.responseCode
    if (rc >= 200 && rc < 300) {
        messageLog?.addAttachmentAsString('SF_Response_OK', "HTTP ${rc}", 'text/plain')
    } else {
        String errTxt = conn.errorStream?.getText('UTF-8') ?: 'n/a'
        throw new RuntimeException("SFSF POST failed (HTTP ${rc}): ${errTxt}")
    }
}

/* =================================================================================
 *  FUNKTION: invalide Records als CSV zusammenstellen
 * ================================================================================= */
private String buildInvalidCsv(List<Map<String,String>> invalids) {
    if (invalids.isEmpty()) { return '' }

    StringBuilder sb = new StringBuilder('PersonID|Validation')
    invalids.each { inv ->
        sb.append('\n')
          .append(inv.PayrollID ?: '')
          .append('|')
          .append(inv.Validation ?: '')
    }
    return sb.toString()
}

/* =================================================================================
 *  FUNKTION: Datumsformat von yyyyMMdd bzw. yyyy-MM-dd nach yyyy-MM-dd
 * ================================================================================= */
private String convertDate(String inDate) {
    if (!inDate) { return '' }
    if (inDate.contains('-')) { return inDate }                           // bereits korrekt
    if (inDate.size() == 8) {
        return "${inDate[0..3]}-${inDate[4..5]}-${inDate[6..7]}"
    }
    return inDate
}

/* =================================================================================
 *  FUNKTION: zentrales Error-Handling
 * ================================================================================= */
private void handleError(String body, Exception e, def messageLog) {
    /*
     * Fügt den Original-Payload als Attachment hinzu und wirft die Exception neu.
     */
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/plain')
    def errorMsg = "Fehler im Script: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}