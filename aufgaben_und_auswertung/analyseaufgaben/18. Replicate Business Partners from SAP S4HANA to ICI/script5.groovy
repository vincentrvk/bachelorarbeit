/****************************************************************************************
 *  Groovy-Skript:  Business Partner-Integration S/4 → ICI (Sell Side)
 *  Autor:         Senior-Integration-Developer
 *  Beschreibung:  – liest einen Business-Partner-Payload
 *                 – validiert die InternalID
 *                 – erstellt das JSON-Request-Payload
 *                 – prüft, ob der BP bereits existiert (GET)
 *                 – erstellt oder aktualisiert (POST /Create | /Update)
 *                 – setzt den BP anschließend auf „active“  (POST /Activate)
 *                 – schreibt umfangreiche Log-Attachments
 *                 – liefert bei Fehlern eine aussagekräftige Exception
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import groovy.xml.XmlUtil
import java.net.HttpURLConnection
import java.net.URL

Message processData(Message message) {
    def messageLog   = messageLogFactory.getMessageLog(message)
    String originalBody = message.getBody(String)

    try {
        /* 1. Header & Property Handling */
        setHeadersAndProperties(message)

        /* 2. Eingangspayload sichern / loggen */
        logPayload(messageLog, 'IncomingPayload', originalBody)

        /* 3. Business Partner splitten & validieren */
        def bpNode = extractValidBusinessPartner(originalBody)
        String supplierCode = bpNode.InternalID?.text() ?: 'placeholder'
        message.setProperty('SupplierCode', supplierCode)

        /* 4. Mapping: XML → JSON */
        logPayload(messageLog, 'BeforeMapping', XmlUtil.serialize(bpNode))
        String jsonPayload = mapBusinessPartner(bpNode)
        message.setBody(jsonPayload)
        logPayload(messageLog, 'AfterMapping', jsonPayload)

        /* 5. Existenz-Check */
        boolean exists = callApiCheckExists(message, messageLog)

        /* 6. Create / Update */
        if (exists) {
            callApiUpdateBP(message, jsonPayload, messageLog)
        } else {
            callApiCreateBP(message, jsonPayload, messageLog)
        }

        /* 7. Aktiv-Schaltung */
        callApiActivate(message, messageLog)

    } catch (Exception e) {
        handleError(originalBody, e, messageLog)
    }
    return message
}

/* =========================================================================
   Modularisierung
   ========================================================================= */

/* Header & Property-Initialisierung */
void setHeadersAndProperties(Message msg) {
    def defaults = [
            requestUser                   : 'placeholder',
            requestPassword               : 'placeholder',
            requestURL                    : 'placeholder',
            RecipientBusinessSystemID_config : 'Icertis_SELL',
            RecipientBusinessSystemID_payload: msg.getHeader('RecipientBusinessSystemID', String) ?: 'placeholder',
            SenderBusinessSystemID        : msg.getHeader('SenderBusinessSystemID', String)      ?: 'placeholder'
    ]
    defaults.each { k, v -> if (msg.getProperty(k) == null) { msg.setProperty(k, v) } }
}

/* Logging Helper */
void logPayload(def msgLog, String name, String payload) {
    msgLog?.addAttachmentAsString(name, payload ?: '', 'text/plain')
}

/* Zentrales Error-Handling */
def handleError(String body, Exception e, def msgLog) {
    msgLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    throw new RuntimeException("Fehler im BP-Skript: ${e.message}", e)
}

/* Business Partner selektieren & validieren */
def extractValidBusinessPartner(String xml) {
    def root = new XmlSlurper().parseText(xml)
    def partners = (root.name() == 'BusinessPartner') ? [root] : root.BusinessPartner
    def valid = partners.find { bp ->
        def numeric = (bp.InternalID?.text() ?: '').replaceAll(/[^0-9]/, '')
        numeric && numeric.toInteger() > 999
    }
    if (!valid) { throw new IllegalArgumentException('Kein Business Partner mit InternalID > 999 gefunden.') }
    return valid
}

/* Mapping XML → JSON (Business Partner) */
String mapBusinessPartner(def bp) {
    boolean isNatural = bp.Common?.NaturalPersonIndicator?.text()?.toBoolean()
    String contractingPartyType = isNatural ? 'Individual' : 'Customer'
    String externalId = isNatural ? (bp.Customer?.InternalID?.text() ?: '') : (bp.InternalID?.text() ?: '')

    String name
    if (isNatural) {
        def given = bp.Common?.Person?.Name?.GivenName?.text() ?: ''
        def family = bp.Common?.Person?.Name?.FamilyName?.text() ?: ''
        def addFam = bp.Common?.Person?.Name?.AdditionalFamilyName?.text() ?: ''
        name = [given, family, addFam].findAll { it }.join(' ').trim()
    } else {
        name = bp.Common?.Organisation?.Name?.FirstLineName?.text() ?: ''
    }

    def roleCodes = bp.Role.collect { it.RoleCode?.text()?.trim() }.findAll { it }
    String country = bp.AddressInformation?.Address?.PostalAddress?.CountryCode?.text() ?: ''

    def builder = new JsonBuilder()
    builder {
        Data {
            ICMContractingPartyType contractingPartyType
            ICMExternalId           externalId
            Name                    name
            syncRequired            roleCodes
            ICMCountryCode {
                DisplayValue        country
            }
        }
    }
    return builder.toPrettyString()
}

/* Basisauth-Header erzeugen */
String basicAuthHeader(Message msg) {
    "${msg.getProperty('requestUser')}:${msg.getProperty('requestPassword')}".bytes.encodeBase64().toString().with { 'Basic ' + it }
}

/* ---------- API-Aufrufe ---------- */

/* GET – Existenz prüfen */
boolean callApiCheckExists(Message msg, def msgLog) {
    String url = "${msg.getProperty('requestURL')}?${msg.getProperty('SupplierCode')}"
    logPayload(msgLog, 'BeforeCheckExists', "GET ${url}")

    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    conn.requestMethod = 'GET'
    conn.setRequestProperty('Authorization', basicAuthHeader(msg))
    conn.connect()

    String response = conn.inputStream?.getText('UTF-8') ?: ''
    logPayload(msgLog, 'AfterCheckExists', response)
    return response.contains('<BusinessPartner')
}

/* POST – Create */
void callApiCreateBP(Message msg, String body, def msgLog) {
    callGenericPost("${msg.getProperty('requestURL')}/Create", body, 'CreateBP', msg, msgLog)
}

/* POST – Update */
void callApiUpdateBP(Message msg, String body, def msgLog) {
    callGenericPost("${msg.getProperty('requestURL')}/Update", body, 'UpdateBP', msg, msgLog)
}

/* POST – Activate */
void callApiActivate(Message msg, def msgLog) {
    String url = "${msg.getProperty('requestURL')}/${msg.getProperty('SupplierCode')}/Activate"
    callGenericPost(url, '', 'ActivateBP', msg, msgLog)
}

/* Generischer POST-Aufruf */
void callGenericPost(String urlStr, String body, String tag, Message msg, def msgLog) {
    logPayload(msgLog, "Before${tag}", body ?: "POST ${urlStr}")

    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.requestMethod = 'POST'
    conn.setRequestProperty('Authorization', basicAuthHeader(msg))
    conn.setRequestProperty('Content-Type', 'application/json')
    conn.doOutput = true
    if (body) { conn.outputStream.withWriter('UTF-8') { it << body } }

    int rc = conn.responseCode
    String response = conn.inputStream?.getText('UTF-8') ?: ''
    logPayload(msgLog, "After${tag}", "HTTP ${rc}\n${response}")
}