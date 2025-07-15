/*****************************************************************************
 * Skript:   ImportBusinessPartnerContacts.groovy
 * Zweck:    Import von Business-Partner-Kontakten aus S/4HANA nach
 *           SAP Field Service Management (FSM) oder Ablage im DataStore
 * Autor:    AI-Assistant
 *****************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.runtime.Factory
import com.sap.it.api.asdk.datastore.*        // DataStoreService, DataBean, DataConfig
import groovy.json.JsonBuilder

import java.net.URLEncoder

// Haupteinstieg des Groovy-Skripts ------------------------------------------------
Message processData(Message message) {

    /* MessageLog für Monitoring holen */
    def msgLog = messageLogFactory.getMessageLog(message)

    /* Body als String sichern (wird auch für Error-Handling benötigt) */
    def rawBody = (message.getBody(String) ?: '')

    try {
        /* 1. Properties & Header auslesen */
        def props = readContextValues(message)

        /* 2. Mapping XML -> JSON durchführen */
        def contactList = mapXmlToContacts(rawBody)

        /* 3. Je nach enableMultiCompany entscheiden */
        if ('true'.equalsIgnoreCase(props.enableMultiCompany)) {
            storeContactsInDataStore(contactList, msgLog)
            /* Ablagebestätigung in Body zurückgeben */
            message.setBody("Contacts stored in DataStore 'BPContacts' (${contactList.size()})")
        } else {
            def responseCodes = sendContactsToFSM(contactList, props, msgLog)
            /* Letzten HTTP-Code in Body zurückgeben */
            message.setBody("Contacts transferred to FSM. Response codes: ${responseCodes}")
        }

        return message

    } catch (Exception e) {
        /* Zentrales Error-Handling */
        handleError(rawBody, e, msgLog)
        /* handleError wirft immer Exception -> folgende Zeile wird nie erreicht
           ist jedoch hier um Compiler-Warnungen zu vermeiden               */
        return message
    }
}

/*---------------------------------------------------------------------------------
 * Funktion: readContextValues
 * Zweck   : Properties und Header aus Message auslesen (oder 'placeholder')
 *--------------------------------------------------------------------------------*/
private Map readContextValues(Message msg) {
    /* Hilfsclosure zum Lesen von Property -> Header -> placeholder */
    def fetch = { String name ->
        msg.getProperty(name) ?:
        msg.getHeader(name, String) ?:
        'placeholder'
    }

    return [
        requestUser       : fetch('requestUser'),
        requestPassword   : fetch('requestPassword'),
        requestURL        : fetch('requestURL'),
        enableMultiCompany: fetch('enableMultiCompany')
    ]
}

/*---------------------------------------------------------------------------------
 * Funktion: mapXmlToContacts
 * Zweck   : Eingangs-XML in Liste von Kontakt-Maps transformieren
 *--------------------------------------------------------------------------------*/
private List<Map> mapXmlToContacts(String xmlString) {

    def slurper = new XmlSlurper()
    def xml     = slurper.parseText(xmlString)

    def contactList = []

    /* Iteration über alle BusinessPartner-Knoten */
    xml.'**'.findAll { it.name() == 'BusinessPartner' }.each { bp ->

        /* Pflichtfeld externalId prüfen */
        String externalId = bp.InternalID.text()
        if (!externalId) {
            throw new IllegalStateException("Kein InternalID (externalId) im Payload vorhanden.")
        }

        /* Kontaktobjekt befüllen */
        def contact = [
            externalId: externalId,
            firstName : bp.Common?.Person?.Name?.GivenName?.text() ?: '',
            lastName  : bp.Common?.Person?.Name?.FamilyName?.text() ?: '',
            inactive  : 'true'.equalsIgnoreCase(bp.Common?.BlockedIndicator?.text()),
            title     : bp.Common?.SalutationText?.text() ?: ''
        ]

        contactList << contact
    }

    return contactList
}

/*---------------------------------------------------------------------------------
 * Funktion: storeContactsInDataStore
 * Zweck   : Jeden Kontakt einzeln im DataStore 'BPContacts' ablegen
 *--------------------------------------------------------------------------------*/
private void storeContactsInDataStore(List<Map> contacts, def msgLog) {

    /* DataStoreService instanziieren */
    def dsService = new Factory(DataStoreService.class).getService()
    if (!dsService) {
        throw new IllegalStateException("DataStoreService konnte nicht instanziiert werden.")
    }

    contacts.each { Map cp ->
        /* Payload: genau ein CP-Objekt verpackt in { "CP":[ {..} ] } */
        def jsonPayload = new JsonBuilder([CP: [cp]]).toString()

        /* DataBean vorbereiten */
        def dBean = new DataBean()
        dBean.setDataAsArray(jsonPayload.getBytes('UTF-8'))

        /* DataConfig vorbereiten */
        def dConfig = new DataConfig()
        dConfig.setStoreName('BPContacts')
        dConfig.setId(cp.externalId)       // Entry-ID = externalId
        dConfig.setOverwrite(true)

        /* Schreiben */
        dsService.put(dBean, dConfig)

        msgLog?.addAttachmentAsString("Stored_${cp.externalId}", jsonPayload, 'application/json')
    }
}

/*---------------------------------------------------------------------------------
 * Funktion: sendContactsToFSM
 * Zweck   : Kontakte per HTTP-POST an FSM senden
 * Rückgabe: Liste der HTTP-Response-Codes
 *--------------------------------------------------------------------------------*/
private List<Integer> sendContactsToFSM(List<Map> contacts, Map props, def msgLog) {

    List<Integer> codes = []

    contacts.each { Map cp ->
        /* URL zusammensetzen  ->  {requestURL}/Contact/{externalId} */
        String url = "${props.requestURL}/Contact/${URLEncoder.encode(cp.externalId,'UTF-8')}"

        /* HTTP-Verbindung aufbauen */
        def conn = new URL(url).openConnection()
        conn.setRequestMethod('POST')
        conn.setDoOutput(true)

        /* Basic-Auth Header */
        String auth = "${props.requestUser}:${props.requestPassword}"
        conn.setRequestProperty('Authorization',
                'Basic ' + auth.bytes.encodeBase64().toString())
        conn.setRequestProperty('Content-Type', 'application/json')

        /* JSON-Body erzeugen und senden ({ "CP":[ {..} ] }) */
        String postBody = new JsonBuilder([CP: [cp]]).toString()
        conn.outputStream.withWriter('UTF-8') { it << postBody }

        /* Response-Code prüfen */
        int rc = conn.responseCode
        codes << rc

        msgLog?.addAttachmentAsString("FSM_Request_${cp.externalId}", postBody, 'application/json')
        msgLog?.addAttachmentAsString("FSM_Response_${cp.externalId}",
                conn.inputStream?.getText('UTF-8') ?: '', 'application/json')

        if (rc != 200) {
            throw new RuntimeException("FSM-Aufruf für ${cp.externalId} fehlgeschlagen (HTTP ${rc}).")
        }
    }

    return codes
}

/*---------------------------------------------------------------------------------
 * Funktion: handleError
 * Zweck   : Zentrales Error-Handling mit Attachment des fehlerhaften Payloads
 *--------------------------------------------------------------------------------*/
private void handleError(String body, Exception e, def messageLog) {
    /* Payload als Attachment anhängen */
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')

    /* Fehlermeldung werfen */
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}