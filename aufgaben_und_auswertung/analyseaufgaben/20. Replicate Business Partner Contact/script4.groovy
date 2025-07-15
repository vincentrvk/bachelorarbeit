/*****************************************************************************************
 * Integration  :  S/4 HANA  ➜  SAP Field Service Management – Business Partner Kontakte
 * Author       :  CPI Groovy Script (modular & reusable)
 * Description  :  1. Liest Properties & Header
 *                 2. Erstellt JSON-Payload für FSM
 *                 3. Je nach enableMultiCompany
 *                       a) POST an FSM  (== "false")
 *                       b) Speicherung pro Kontakt im DataStore (== "true")
 *                 4. Durchgängiges Error-Handling & Logging
 *****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.*

// =========================  MAIN  =======================================================
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1) Properties & Header lesen / setzen */
        def params = prepareContext(message, [
                'requestUser',
                'requestPassword',
                'requestURL',
                'enableMultiCompany'
        ])

        /* 2) Mapping XML  ➜  JSON */
        String inputXml = message.getBody(String) ?: ''
        List<Map> cpList  = mapBusinessPartners(inputXml)

        /* 3) Multi-Company Entscheidung */
        if (params.enableMultiCompany.toLowerCase() == 'true') {
            persistPerContact(cpList)
        } else {
            postContactsToFSM(cpList, params)
        }

        /* 4) Ergebnis in Message Body ablegen (nur zu Demonstrationszwecken) */
        message.setBody(JsonOutput.toJson([CP: cpList]))
        return message

    } catch (Exception e) {
        /* zentrales Error-Handling */
        handleError(message.getBody(String) ?: '', e, messageLog)
        return message    // wird wegen throw in handleError nie erreicht, IDE-Ruhe
    }
}

// =========================  FUNCTIONS  ==================================================
/*-----------------------------------------------------------------------------
 * Bereitet Kontext‐Variablen (Header & Properties) auf und ersetzt fehlende
 * Einträge durch "placeholder".
 *---------------------------------------------------------------------------*/
private Map prepareContext(Message msg, List<String> keys) {

    Map<String, String> result = [:]
    keys.each { String key ->
        def val = msg.getProperty(key)
        if (val == null) {                               // nicht vorhanden → placeholder
            msg.setProperty(key, 'placeholder')
            val = 'placeholder'
        }
        result[key] = val.toString()
    }
    return result
}

/*-----------------------------------------------------------------------------
 * XML → JSON Mapping gemäss Vorgaben.
 *---------------------------------------------------------------------------*/
private List<Map> mapBusinessPartners(String xmlTxt) {

    def xml = new XmlSlurper(false, false).parseText(xmlTxt)

    // Unabhängig von evtl. Namensräumen alle BusinessPartner finden
    def bpNodes = xml.'**'.findAll { it.name() == 'BusinessPartner' }

    if (!bpNodes) {
        throw new IllegalStateException('Keine BusinessPartner-Knoten im eingehenden XML gefunden.')
    }

    List<Map> cpList = []

    bpNodes.each { bp ->
        String externalId = (bp.InternalID.text() ?: '').trim()
        if (!externalId) {
            throw new IllegalStateException('Pflichtfeld InternalID fehlt im BusinessPartner.')
        }

        Map cp = [
                externalId : externalId,
                firstName  : (bp.Common?.Person?.Name?.GivenName?.text() ?: '').trim(),
                lastName   : (bp.Common?.Person?.Name?.FamilyName?.text() ?: '').trim(),
                inactive   : ((bp.Common?.BlockedIndicator?.text() ?: '').toLowerCase() == 'true'),
                title      : (bp.Common?.SalutationText?.text() ?: '')
        ]
        cpList << cp
    }
    return cpList
}

/*-----------------------------------------------------------------------------
 * Persistiert jeden Kontakt einzeln im DataStore "BPContacts"
 * Store-Entry-ID = externalId
 *---------------------------------------------------------------------------*/
private void persistPerContact(List<Map> contacts) {

    // Service‐Instanz holen
    def dsService = new Factory(DataStoreService.class).getService()
    if (dsService == null) {
        throw new IllegalStateException('DataStoreService konnte nicht instanziiert werden.')
    }

    contacts.each { Map cp ->
        DataBean db = new DataBean()
        db.setDataAsArray(JsonOutput.toJson(cp).getBytes('UTF-8'))

        DataConfig dc = new DataConfig()
        dc.setStoreName('BPContacts')
        dc.setId(cp.externalId)
        dc.setOverwrite(true)

        dsService.put(db, dc)
    }
}

/*-----------------------------------------------------------------------------
 * Sendet kompletten CP-Array an FSM (POST)
 *---------------------------------------------------------------------------*/
private void postContactsToFSM(List<Map> contacts, Map params) {

    String endpoint = "${params.requestURL}/Contact/externalId"
    String payload  = JsonOutput.toJson([CP: contacts])

    def urlConn = new URL(endpoint).openConnection()
    urlConn.setRequestMethod('POST')
    urlConn.setDoOutput(true)

    // Basic-Auth Header
    String auth = "${params.requestUser}:${params.requestPassword}"
    urlConn.setRequestProperty('Authorization', 'Basic ' + auth.bytes.encodeBase64().toString())
    urlConn.setRequestProperty('Content-Type', 'application/json; charset=UTF-8')

    urlConn.outputStream.withWriter('UTF-8') { it << payload }

    int rc = urlConn.responseCode
    if (rc != 200) {
        String errTxt = "FSM-API Rückgabecode != 200 (war ${rc})."
        throw new RuntimeException(errTxt)
    }
}

/*-----------------------------------------------------------------------------
 * Zentrales Error-Handling gemäss Vorgabe – hängt Payload als Attachment an.
 *---------------------------------------------------------------------------*/
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/plain')
    def errorMsg = "Fehler im Mapping-/Integrations-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}