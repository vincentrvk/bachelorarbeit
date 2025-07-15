/*****************************************************************************************
*  Skriptname  :  BPContactIntegration.groovy
*  Beschreibung:  Mapping & Versand bzw. Ablage von Business Partner Kontakten aus S/4-HANA
*                 nach SAP Field Service Management (FSM). 
*                 – Multi-Company “true”  ⇒ Ablage im DataStore „BPContacts“
*                 – Multi-Company “false” ⇒ Aufruf FSM-API  /Contact/externalId (POST)
*
*  Autorenhinweis:  Erstellt von ChatGPT (Senior-Entwickler-Perspektive)
*****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import com.sap.it.api.asdk.runtime.Factory
import com.sap.it.api.asdk.datastore.*   // DataStoreService, DataBean, DataConfig
import java.nio.charset.StandardCharsets

Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    String originalPayload = message.getBody(String)                             // Eingehender XML-Payload

    try {
        /*-----------------------------------------------------------
        *  1. Properties & Header Werte ermitteln (oder Placeholder)
        *----------------------------------------------------------*/
        def cfg = initConfig(message)

        /*-----------------------------------------------------------
        *  2. XML  →  JSON Mapping
        *----------------------------------------------------------*/
        String fsmPayload = buildRequestPayload(originalPayload)

        /*-----------------------------------------------------------
        *  3. Multi-Company Entscheidung
        *----------------------------------------------------------*/
        if (cfg.enableMultiCompany.toString().equalsIgnoreCase('true')) {
            saveToDataStore(fsmPayload, messageLog)                               // Ablage im DataStore
        } else {
            callFsmCreateContact(cfg, fsmPayload, messageLog)                     // API-Aufruf
        }

        /*-----------------------------------------------------------
        *  4. Ergebnis in den Message-Body legen (Debug/Weiterverarb.)
        *----------------------------------------------------------*/
        message.setBody(fsmPayload)                                               // Für Folgeschritte verfügbar
        return message

    } catch (Exception e) {
        handleError(originalPayload, e, messageLog)                               // Zentrales Error-Handling
    }
}



/* =========================================================================
 *  Funktionsbibliothek – Modularer Aufbau
 * ========================================================================= */

/**
 * Liest benötigte Configuration-Werte aus den Message-Properties bzw. -Headern.
 * Fehlt ein Wert, wird er mit 'placeholder' belegt.
 */
def initConfig(Message msg) {
    def safeRead = { String key, boolean isProperty ->
        def val = isProperty ? msg.getProperty(key) : msg.getHeader(key, String)
        return val ? val.toString() : 'placeholder'
    }
    return [
        requestUser        : safeRead('requestUser',        true),
        requestPassword    : safeRead('requestPassword',    true),
        requestURL         : safeRead('requestURL',         true),
        enableMultiCompany : safeRead('enableMultiCompany', true)
    ]
}

/**
 * Erstellt das FSM-Request-Payload (JSON) aus dem eingehenden XML.
 * Transformation laut Mappingvorgabe.
 */
String buildRequestPayload(String xmlBody) {

    def xml = new XmlSlurper().parseText(xmlBody)

    // XmlPfad-Navigation – nur erstes BusinessPartner-Objekt berücksichtigt
    def bp               = xml.'**'.find { it.name() == 'BusinessPartner' }
    def internalId       = bp?.InternalID?.text() ?: ''
    def givenName        = bp?.Common?.Person?.Name?.GivenName?.text() ?: ''
    def familyName       = bp?.Common?.Person?.Name?.FamilyName?.text() ?: ''
    def blockedIndicator = bp?.Common?.BlockedIndicator?.text()
    def salutation       = bp?.Common?.SalutationText?.text()

    // Konvertierung BlockedIndicator → boolean
    boolean inactive = blockedIndicator?.equalsIgnoreCase('true')

    // JSON-Generierung
    def jsonBuilder = new JsonBuilder()
    jsonBuilder {
        CP ([
            [
                externalId : internalId,
                firstName  : givenName,
                lastName   : familyName,
                inactive   : inactive,
                title      : (salutation ?: '')
            ]
        ])
    }
    return jsonBuilder.toPrettyString()
}

/**
 * Führt den HTTP-Aufruf der FSM-API 'Create Contact Person' aus.
 * Wirft Exception, falls HTTP-Status ≠ 200.
 */
void callFsmCreateContact(Map cfg, String payload, def messageLog) {

    /*---------- URL & Connection vorbereiten ----------*/
    String targetUrl = "${cfg.requestURL}/Contact/externalId"
    URLConnection conn = new URL(targetUrl).openConnection()
    String basicAuth   = "${cfg.requestUser}:${cfg.requestPassword}"
                          .getBytes(StandardCharsets.UTF_8)
                          .encodeBase64()
                          .toString()

    conn.setRequestMethod('POST')
    conn.setRequestProperty('Authorization', "Basic ${basicAuth}")
    conn.setRequestProperty('Content-Type', 'application/json')
    conn.doOutput = true
    conn.connectTimeout = 30000
    conn.readTimeout    = 30000

    /*---------- Payload senden ----------*/
    conn.outputStream.withWriter { it << payload }

    int httpCode = conn.responseCode
    if (httpCode != 200) {
        String errResp = conn.errorStream ? conn.errorStream.text : ''
        messageLog?.addAttachmentAsString('FSM-ErrorResponse', errResp, 'text/plain')
        throw new RuntimeException("FSM-API Rückgabecode ${httpCode} ungleich 200")
    }

    /*---------- Erfolgs-Logging ----------*/
    messageLog?.setStringProperty('FSM-API-Status', httpCode.toString())
}

/**
 * Speichert den Payload in den DataStore 'BPContacts'.
 * Ein Eintrag je CP-Objekt (Entry-ID = externalId).
 */
void saveToDataStore(String jsonPayload, def messageLog) {

    def service = new Factory(DataStoreService.class).getService()
    if (service == null) {
        throw new RuntimeException('DataStoreService konnte nicht instanziiert werden.')
    }

    def parsed = new groovy.json.JsonSlurper().parseText(jsonPayload)
    parsed.CP.each { cpObj ->

        DataBean   bean   = new DataBean()
        bean.setDataAsArray(new JsonBuilder([CP:[cpObj]]).toString()
                            .getBytes(StandardCharsets.UTF_8))

        DataConfig config = new DataConfig()
        config.setStoreName('BPContacts')
        config.setId(cpObj.externalId?.toString() ?: UUID.randomUUID().toString())
        config.setOverwrite(true)

        service.put(bean, config)
        messageLog?.addAttachmentAsString("DS-Entry-${config.getId()}",
                                          new String(bean.getDataAsArray(), 'UTF-8'),
                                          'application/json')
    }
}

/**
 * Zentrales Error-Handling. Hängt den Original-Payload als Attachment an
 * das MPL an und wirft eine RuntimeException zur Abbruchsignalisierung.
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im BPContactIntegration-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}