/********************************************************************************************
*  Skript:  Create / Persist SAP FSM Contact Persons
*  Autor:   AI Assistant – Senior Integration Developer
*  Zweck:   - Mapping von S/4 HANA Business Partner Kontakten zu SAP FSM „Contact Person“
*           - Entweder direkter Aufruf der FSM-API oder Zwischenspeichern im Datastore
*  Hinweise: Alle Funktionen sind modular aufgebaut und verfügen über Error-Handling.
********************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import com.sap.it.api.asdk.datastore.*          // DataStoreService, DataBean, DataConfig, …
import com.sap.it.api.asdk.runtime.Factory      // Service Factory

/***************************************
*  Haupteinstieg der IFlow-Groovy-Step
***************************************/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1. Kontext ermitteln (Header/Properties lesen) */
        def ctx = retrieveContextValues(message)

        /* 2. Mapping S/4 XML  ➜  JSON (FSM) */
        def cpList      = mapCpObjects(message.getBody(String) ?: '')
        def jsonPayload = new JsonBuilder([CP: cpList]).toPrettyString()

        /* 3. Entscheidung – Multi-Company oder direkte API-Übertragung */
        if (ctx.enableMultiCompany == 'true') {
            persistInDataStore(cpList, messageLog)
        } else {
            callCreateContactPerson(jsonPayload, ctx, messageLog)
        }

        /* 4. Ergebnis als Body weiterreichen */
        message.setBody(jsonPayload)
        return message

    } catch (Exception e) {
        /* zentrales Error-Handling */
        handleError(message.getBody(String) ?: '', e, messageLog)
        return message          // wird bei handleError() nie erreicht, nötig für Groovy
    }
}

/*****************************************************************
*  Funktion: Kontextwerte aus Header/Properties ermitteln
*****************************************************************/
private Map<String, String> retrieveContextValues(Message message) {

    /* Helper Closure – holt Wert zuerst aus Property dann Header */
    def getVal = { String key ->
        def val = message.getProperty(key) ?: message.getHeader(key, String)
        (val == null || val.toString().trim().isEmpty()) ? 'placeholder' : val.toString().trim()
    }

    return [
        requestUser        : getVal('requestUser'),
        requestPassword    : getVal('requestPassword'),
        requestURL         : getVal('requestURL'),
        enableMultiCompany : getVal('enableMultiCompany').toLowerCase()
    ]
}

/*****************************************************************
*  Funktion: Mapping XML ➜ CP-Liste
*****************************************************************/
private List<Map> mapCpObjects(String xmlPayload) {

    if (!xmlPayload) {
        throw new IllegalArgumentException('Leerer XML-Payload übergeben.')
    }

    def cpObjects = []

    /* XML parsen (Namespaces werden ignoriert) */
    def root = new XmlSlurper().parseText(xmlPayload)

    root.'**'.findAll { it.name() == 'BusinessPartner' }.each { bpNode ->

        def externalId = bpNode.InternalID.text().trim()
        if (!externalId) {
            throw new IllegalStateException('Pflichtfeld "externalId" (InternalID) fehlt.')
        }

        def blocked   = bpNode.Common.BlockedIndicator.text()?.trim()
        def salutation = bpNode.Common.SalutationText.text()?.trim()

        cpObjects << [
            externalId : externalId,
            firstName  : bpNode.Common.Person.Name.GivenName.text()?.trim() ?: '',
            lastName   : bpNode.Common.Person.Name.FamilyName.text()?.trim() ?: '',
            inactive   : (blocked == 'true'),
            title      : salutation ?: ''
        ]
    }

    if (cpObjects.isEmpty()) {
        throw new IllegalStateException('Keine BusinessPartner-Einträge im XML gefunden.')
    }

    return cpObjects
}

/*****************************************************************
*  Funktion: Persistieren in DataStore (Multi-Company = true)
*****************************************************************/
private void persistInDataStore(List<Map> cpList, def messageLog) {

    /* DataStoreService Instanz holen */
    def dsService = new Factory(DataStoreService.class).getService()
    if (dsService == null) {
        throw new IllegalStateException('DataStoreService konnte nicht instanziiert werden.')
    }

    cpList.each { Map cp ->
        def dataBean  = new DataBean()
        def payload   = new JsonBuilder(cp).toString().getBytes(StandardCharsets.UTF_8)
        dataBean.setDataAsArray(payload)

        def cfg = new DataConfig()
        cfg.setStoreName('BPContacts')
        cfg.setId(cp.externalId)
        cfg.setOverwrite(true)

        dsService.put(dataBean, cfg)      // Persistieren
        messageLog?.addAttachmentAsString(
                "Persisted_${cp.externalId}",
                new String(payload, StandardCharsets.UTF_8),
                'application/json')
    }
}

/*****************************************************************
*  Funktion: FSM-API Aufruf (Multi-Company = false)
*****************************************************************/
private void callCreateContactPerson(String jsonPayload,
                                     Map<String, String> ctx,
                                     def messageLog) {

    def urlStr = "${ctx.requestURL}/Contact/externalId"
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()

    /* HTTP Request konfigurieren */
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Content-Type', 'application/json')
    String basicAuth = "${ctx.requestUser}:${ctx.requestPassword}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${basicAuth}")

    /* Body senden */
    conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { writer ->
        writer << jsonPayload
    }

    int responseCode = conn.responseCode
    if (responseCode != 200) {
        String errBody = conn.errorStream?.getText(StandardCharsets.UTF_8.name()) ?: ''
        throw new RuntimeException("FSM-API Fehler – HTTP ${responseCode}: ${errBody}")
    }

    /* Erfolgs-Log */
    messageLog?.addAttachmentAsString('FSM_Response', conn.inputStream.text, 'application/json')
}

/*****************************************************************
*  Funktion: Zentrales Error-Handling
*****************************************************************/
private void handleError(String body, Exception e, def messageLog) {
    /* 1. Payload als Anhang sichern */
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '<empty>', 'text/plain')

    /* 2. Fehlermeldung werfen – dadurch Message Processing Log als failed markiert */
    def errorMsg = "Fehler im Mapping-/Verarbeitungs-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}