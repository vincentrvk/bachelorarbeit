/****************************************************************************************
 *  Groovy-Script : Import Employees to SAP SuccessFactors Employee Central Payroll
 *  Author        : CPI – Senior Integration Developer
 *  Description   : 1. Read CSV payload (pipe separated) from message body
 *                  2. Validate every record
 *                  3. Map valid records to XML according to target XSD
 *                  4. POST every single record to SuccessFactors (Basic-Auth)
 *                  5. Collect all invalid records in CSV form (PersonID|Validation)
 *                  6. Comprehensive error handling incl. payload attachment
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import groovy.xml.StreamingMarkupBuilder
import java.text.SimpleDateFormat
import java.net.HttpURLConnection
import java.net.URL
import java.util.Base64

/* ============================================================
 *               MAIN ENTRY POINT – processData()
 * ============================================================*/
Message processData(Message message) {

    //--- Preparation ---------------------------------------------------  
    final def msgLog = messageLogFactory.getMessageLog(message)
    String body            = message.getBody(String) ?: ''
    Map<String,String> cfg = getConfiguration(message)          //Properties / Headers
    
    //--- Collections ---------------------------------------------------
    List<Map> validRecords   = []
    List<Map> invalidRecords = []

    try {
        //---------------------------------------------------------------
        // 1. Read & Split CSV
        //---------------------------------------------------------------
        List<String> lines = body.readLines().findAll { it?.trim() }   //remove empty lines
        if(lines.isEmpty()) throw new IllegalArgumentException('CSV Payload leer.')

        String headerLine  = lines.head()
        List<String> csvHeader = headerLine.split('\\|') as List
        lines.tail().each { line ->
            Map<String,String> record = parseCsvLine(line, csvHeader)
            List<String> errors       = validateRecord(record)
            if(errors.isEmpty())   { validRecords   << record }
            else                   { record['__validation'] = errors.join(', '); invalidRecords << record }
        }

        //---------------------------------------------------------------
        // 2. Build XML for every valid record & POST to SuccessFactors
        //---------------------------------------------------------------
        validRecords.each { Map r ->
            String xmlRecord = mapToXml(r)
            postToSuccessFactors(xmlRecord, cfg, msgLog)
        }

        //---------------------------------------------------------------
        // 3. Build invalid CSV (if any) and put into body
        //---------------------------------------------------------------
        if(!invalidRecords.isEmpty()){
            String invalidCsv = buildInvalidCsv(invalidRecords)
            message.setBody(invalidCsv)
            message.setHeader('Content-Type', 'text/csv')
        }else{
            message.setBody('All records successfully transferred.')
        }

    } catch(Exception e){
        handleError(body, e, msgLog)
    }

    return message
}

/* ============================================================
 *                    HELPER / UTILITY METHODS
 * ============================================================*/

/* ------------------------------------------------------------
 *  Parse single CSV line into Map
 * -----------------------------------------------------------*/
private Map<String,String> parseCsvLine(String line, List<String> header){
    List<String> columns = line.split('\\|', -1) as List   //-1 keeps empty fields
    Map<String,String> record = [:]
    header.eachWithIndex{ String h, int idx -> record[h.trim()] = columns[idx]?.trim() }
    return record
}

/* ------------------------------------------------------------
 *  Validate one record – returns list of error messages
 * -----------------------------------------------------------*/
private List<String> validateRecord(Map r){
    List<String> errors = []

    if(!r.ValidityPeriod_StartDate) errors << 'Missing Start Date'
    if(!r.ValidityPeriod_EndDate)   errors << 'Missing End Date'
    if(!r.CompenstationComponentTypeID)  errors << 'Missing Component Type'
    if(!r.Amount)                   errors << 'Missing Amount'
    if(!r.PayrollID)                errors << 'Missing PayrollID'
    else {
        if(r.PayrollID.length() >= 9)                         errors << 'PayrollID too long'
        if(!(r.PayrollID =~ /^\d+$/))                         errors << 'PayrollID not numeric'
    }
    if(r.Record_Type?.equalsIgnoreCase('E')) errors << 'RecordType E not allowed'

    return errors
}

/* ------------------------------------------------------------
 *  Build XML for one single record
 * -----------------------------------------------------------*/
private String mapToXml(Map r){
    def dateFmtIn   = ['yyyy-MM-dd','yyyyMMdd']
    String start    = normalizeDate(r.ValidityPeriod_StartDate, dateFmtIn)
    String end      = normalizeDate(r.ValidityPeriod_EndDate  , dateFmtIn)

    def sw = new StringWriter()
    new MarkupBuilder(sw).'EmployeeBenefitsVendorData'{                                                                      //Root element per record
        'EmployeeID'(r.PayrollID.toInteger())
        'EmployeeBenefitData'{
            'ValidityPeriod'{
                'StartDate'(start)
                'EndDate'  (end)
            }
            'CompensationComponentTypeID'(r.CompenstationComponentTypeID)
            'Amount'     ('currencyCode':'USD',      r.Amount)
            'GoalAmount' ('currencyCode':'USD',      r.GoalAmount ?: '0')
            'PercentageContribution'( (r.PercentageContribution ?: '0').replace('%','') )
        }
    }
    return sw.toString()
}

/* ------------------------------------------------------------
 *  POST one XML record to SuccessFactors
 * -----------------------------------------------------------*/
private void postToSuccessFactors(String xmlPayload, Map cfg, def msgLog){
    try{
        URL url = new URL(cfg.url)                                         //Endpoint URL
        HttpURLConnection con = (HttpURLConnection) url.openConnection()
        con.with{
            setRequestMethod('POST')
            setRequestProperty('Authorization', 'Basic ' + cfg.auth)
            setRequestProperty('Content-Type', 'application/xml')
            setDoOutput(true)
            outputStream.withWriter('UTF-8'){ it << xmlPayload }
        }
        int rc = con.responseCode
        msgLog?.addAttachmentAsString("SF_Response_${System.currentTimeMillis()}",
                                      "HTTP ${rc} – ${con.responseMessage}",
                                      'text/plain')
        if(rc >= 400) throw new RuntimeException("SF-Request failed with HTTP ${rc}")
    }catch(Exception ex){
        throw ex          //will be handled in outer try/catch
    }
}

/* ------------------------------------------------------------
 *  Build CSV for invalid records
 * -----------------------------------------------------------*/
private String buildInvalidCsv(List<Map> invalidRecords){
    StringBuilder sb = new StringBuilder('PersonID|Validation')
    invalidRecords.each{
        sb << '\n' << (it.PayrollID ?: '') << '|' << it.__validation
    }
    return sb.toString()
}

/* ------------------------------------------------------------
 *  Extract properties / headers or fallback to placeholder
 * -----------------------------------------------------------*/
private Map<String,String> getConfiguration(Message m){
    Map<String,Object> props   = m.getProperties()
    Map<String,Object> headers = m.getHeaders()

    String user = props.successFactorsUsername ?: headers.successFactorsUsername ?: 'placeholder'
    String pw   = props.successFactorsPassword ?: headers.successFactorsPassword ?: 'placeholder'
    String url  = props.successFactorsURL      ?: headers.successFactorsURL      ?: 'placeholder'

    String auth = Base64.getEncoder().encodeToString("${user}:${pw}".getBytes('UTF-8'))
    return [user:user, pw:pw, url:url, auth:auth]
}

/* ------------------------------------------------------------
 *  Convert various date patterns to yyyy-MM-dd
 * -----------------------------------------------------------*/
private String normalizeDate(String dateStr, List<String> possiblePatterns){
    possiblePatterns.findResult { pat ->
        try {
            def sdfIn  = new SimpleDateFormat(pat)
            sdfIn.setLenient(false)
            Date d = sdfIn.parse(dateStr)
            return new SimpleDateFormat('yyyy-MM-dd').format(d)
        }catch(Exception ignored){ null }
    } ?: ''
}

/* ------------------------------------------------------------
 *  Central Error Handler – attaches payload & rethrows
 * -----------------------------------------------------------*/
private void handleError(String body, Exception e, def messageLog){
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/plain')
    String msg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(msg, e)
}