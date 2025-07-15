/****************************************************************************************
 * Skript:    CreateOrStoreContactPerson.groovy
 * Zweck:     Erstellt Business-Partner-Kontakte in SAP Field Service Management
 *            oder speichert diese – bei Multi-Company-Szenarien – in einem Datastore.
 * Autor:     AI-Assistant
 * Hinweis:   Alle Funktionen sind modular aufgebaut, vollständig kommentiert (DE)
 *            und enthalten aussagekräftiges Error-Handling.
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.*
import groovy.json.JsonBuilder
import java.net.HttpURLConnection

// ----------------------------------------------------------------------------
//  Haupt-Einstiegspunkt für das Groovy-Skript (CPI Standard)
// ----------------------------------------------------------------------------
Message processData(Message message) {
    // Initiale Objekte
    def body        = message.getBody(String) ?: ''
    def messageLog  = messageLogFactory.getMessageLog(message)

    try {
        // 1. Header & Property-Werte ermitteln
        def cfg = readConfiguration(message)

        // 2. Mapping XML -> CP-Objektliste
        List<Map> cpObjects = mapXmlToCpObjects(body)

        // 3. Validierung der Pflichtfelder
        validateCpObjects(cpObjects)

        // 4. Multi-Company Entscheidung
        if (cfg.enableMultiCompany.toBoolean()) {
            persistCpObjects(cpObjects, messageLog)
            message.setBody('Contacts wurden im Datastore abgelegt.')
        } else {
            String requestPayload = new JsonBuilder([CP: cpObjects]).toString()
            int httpCode = callCreateContactApi(cfg, requestPayload, messageLog)
            message.setBody("Kontakt erfolgreich angelegt. HTTP-Code: ${httpCode}")
        }

    } catch (Exception e) {
        handleError(body, e, messageLog)
    }
    return message
}

// ----------------------------------------------------------------------------
//  Liest benötigte Properties & Header in ein Konfigurations-Map
// ----------------------------------------------------------------------------
private Map readConfiguration(Message msg) {
    [
        requestUser        : (msg.getProperty('requestUser')        ?: 'placeholder') as String,
        requestPassword    : (msg.getProperty('requestPassword')    ?: 'placeholder') as String,
        requestURL         : (msg.getProperty('requestURL')         ?: 'placeholder') as String,
        enableMultiCompany : (msg.getProperty('enableMultiCompany') ?: 'false')       as String
    ]
}

// ----------------------------------------------------------------------------
//  Wandelt das eingehende XML in eine Liste von CP-Objekten um
// ----------------------------------------------------------------------------
private List<Map> mapXmlToCpObjects(String xmlString) {
    def xml = new XmlSlurper().parseText(xmlString)
    def cpList = []

    xml.'**'.findAll { it.name() == 'BusinessPartner' }.each { bp ->
        Map<String, Object> cp = [:]
        cp.externalId = bp.InternalID.text()
        cp.firstName  = bp.Common.Person?.Name?.GivenName?.text()    ?: ''
        cp.lastName   = bp.Common.Person?.Name?.FamilyName?.text()   ?: ''
        String blocked = bp.Common.BlockedIndicator.text()
        cp.inactive   = blocked == 'true'
        cp.title      = bp.Common.SalutationText.text()              ?: ''
        cpList << cp
    }
    return cpList
}

// ----------------------------------------------------------------------------
//  Prüft Pflichtfelder gem. Zielschema
// ----------------------------------------------------------------------------
private void validateCpObjects(List<Map> cpObjs) {
    cpObjs.each { cp ->
        if (!cp.externalId) {
            throw new IllegalArgumentException('Pflichtfeld "externalId" fehlt im Mapping-Ergebnis.')
        }
    }
}

// ----------------------------------------------------------------------------
//  Persistiert CP-Objekte einzeln in den Datastore „BPContacts”
// ----------------------------------------------------------------------------
private void persistCpObjects(List<Map> cpObjs, def messageLog) {
    def service = new Factory(DataStoreService.class).getService()
    if (service == null) {
        throw new IllegalStateException('DataStoreService konnte nicht instanziiert werden.')
    }

    cpObjs.each { cp ->
        def dataBean = new DataBean()
        dataBean.setDataAsArray(new JsonBuilder(cp).toString().getBytes('UTF-8'))

        def dataCfg = new DataConfig()
        dataCfg.setStoreName('BPContacts')
        dataCfg.setId(cp.externalId)
        dataCfg.setOverwrite(true)

        service.put(dataBean, dataCfg)
        messageLog?.addAttachmentAsString("Datastore-Eintrag ${cp.externalId}", new String(dataBean.getDataAsArray()), 'application/json')
    }
}

// ----------------------------------------------------------------------------
//  Ruft die FSM-API auf, um Kontakte anzulegen
// ----------------------------------------------------------------------------
private int callCreateContactApi(Map cfg, String payload, def messageLog) {
    String endpoint = "${cfg.requestURL}/Contact/externalId"
    HttpURLConnection conn = (HttpURLConnection) new URL(endpoint).openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)

    // Basic-Auth Header setzen
    String auth = "${cfg.requestUser}:${cfg.requestPassword}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${auth}")
    conn.setRequestProperty('Content-Type', 'application/json;charset=UTF-8')

    // Body senden
    conn.outputStream.withWriter('UTF-8') { it << payload }

    int responseCode = conn.responseCode
    messageLog?.addAttachmentAsString('FSM-Response', conn.inputStream?.text ?: '', 'text/plain')

    if (responseCode != 200) {
        throw new RuntimeException("FSM-Service antwortete mit HTTP-Code ${responseCode}")
    }
    return responseCode
}

// ----------------------------------------------------------------------------
//  Einheitliches Error-Handling (inkl. Payload als Attachment)
// ----------------------------------------------------------------------------
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String errorMsg = "Fehler im Contact-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}