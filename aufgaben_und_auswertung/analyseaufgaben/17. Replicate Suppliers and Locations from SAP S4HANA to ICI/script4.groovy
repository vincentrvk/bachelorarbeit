/********************************************************************************************
 * Groovy-Skript:  BusinessPartner_2_ICI.groovy
 *
 * Zweck            :  Verarbeitung, Validierung, Mapping und Übertragung von Business-
 *                     Partnern (inkl. Adressen) aus S/4 HANA in ICI (Buy-Side).
 *
 * Autor            :  AI-Assistant – Senior-Integration-Developer
 * SAP CI Version   :  Cloud Integration (Neo & CF)
 ********************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import groovy.xml.XmlUtil

/* ============================================================
 * Haupt­eintrittspunkt des Skriptes
 * ========================================================== */
Message processData(Message message) {

    /* -------- Logging-Instanz holen ------------------------ */
    def messageLog = messageLogFactory.getMessageLog(message)

    try {

        /* -------- Header & Properties setzen ---------------- */
        setHeadersAndProperties(message, messageLog)

        /* -------- Eingehenden Payload sichern --------------- */
        def originalBody = message.getBody(String) ?: ''
        message.setProperty('originalPayload', originalBody)
        logAttachment(messageLog, 'INCOMING_XML', originalBody, 'text/xml')

        /* -------- XML parsen -------------------------------- */
        def root = new XmlSlurper().parseText(originalBody)

        /* -------- Validierung: es muss mind. 1 BP > 999 geben */
        def validBPs = root.BusinessPartner.findAll { bp ->
            bp?.InternalID?.text()?.isInteger() && bp.InternalID.text().toInteger() > 999
        }
        if (!validBPs || validBPs.isEmpty()) {
            throw new Exception('Keine BusinessPartner mit InternalID > 999 gefunden.')
        }

        /* ====================================================
         * Verarbeitung je Business Partner
         * ================================================== */
        validBPs.each { bp ->
            def supplierCode = bp.InternalID.text()
            message.setProperty('SupplierCode', supplierCode)

            /* ---------- Business Partner Mapping ------------- */
            logAttachment(messageLog, "BP_${supplierCode}_BEFORE_MAPPING", XmlUtil.serialize(bp), 'text/xml')
            String bpJson = buildBusinessPartnerPayload(bp, message)
            logAttachment(messageLog, "BP_${supplierCode}_AFTER_MAPPING", bpJson, 'application/json')

            /* ---------- Existenzprüfung Business Partner ----- */
            String baseUrl = message.getProperty('requestURL') as String
            String user    = message.getProperty('requestUser') as String
            String pass    = message.getProperty('requestPassword') as String

            logAttachment(messageLog, "BP_${supplierCode}_CHECK_REQUEST", "${baseUrl}?${supplierCode}", 'text/plain')
            def checkResp = callCheckBusinessPartnerExists(baseUrl, supplierCode, user, pass)
            logAttachment(messageLog, "BP_${supplierCode}_CHECK_RESPONSE", checkResp, 'application/json')

            /* ---------- Create | Update ---------------------- */
            if (!checkResp || !checkResp.contains('"Data"')) {
                /* -------- CREATE -------- */
                logAttachment(messageLog, "BP_${supplierCode}_CREATE_REQUEST", bpJson, 'application/json')
                callCreateBusinessPartner(baseUrl, user, pass, bpJson)
                logAttachment(messageLog, "BP_${supplierCode}_CREATE_RESPONSE", 'CREATED', 'text/plain')
            } else {
                /* -------- UPDATE -------- */
                logAttachment(messageLog, "BP_${supplierCode}_UPDATE_REQUEST", bpJson, 'application/json')
                callUpdateBusinessPartner(baseUrl, user, pass, bpJson)
                logAttachment(messageLog, "BP_${supplierCode}_UPDATE_RESPONSE", 'UPDATED', 'text/plain')
            }

            /* ---------- Aktivierung -------------------------- */
            logAttachment(messageLog, "BP_${supplierCode}_ACTIVATE_REQUEST", "${baseUrl}/${supplierCode}/Activate", 'text/plain')
            callSetActiveStatus(baseUrl, supplierCode, user, pass)
            logAttachment(messageLog, "BP_${supplierCode}_ACTIVATE_RESPONSE", 'ACTIVATED', 'text/plain')

            /* =================================================
             *  Verarbeitung der Adressen
             * =============================================== */
            bp.AddressInformation.each { addr ->
                def addressUUID = addr.UUID.text()
                message.setProperty('AddressUUID', addressUUID)

                /* ------ Address Mapping ---------------------- */
                logAttachment(messageLog, "ADDR_${addressUUID}_BEFORE_MAPPING", XmlUtil.serialize(addr), 'text/xml')
                String addrJson = buildAddressPayload(addr, message)
                logAttachment(messageLog, "ADDR_${addressUUID}_AFTER_MAPPING", addrJson, 'application/json')

                /* ------ Existenzprüfung Adresse -------------- */
                logAttachment(messageLog, "ADDR_${addressUUID}_CHECK_REQUEST", "${baseUrl}?${addressUUID}", 'text/plain')
                def addrCheck = callCheckAddressExists(baseUrl, addressUUID, user, pass)
                logAttachment(messageLog, "ADDR_${addressUUID}_CHECK_RESPONSE", addrCheck, 'application/json')

                if (!addrCheck || !addrCheck.contains('"Data"')) {
                    /* -- CREATE -- */
                    logAttachment(messageLog, "ADDR_${addressUUID}_CREATE_REQUEST", addrJson, 'application/json')
                    callCreateAddress(baseUrl, user, pass, addrJson)
                    logAttachment(messageLog, "ADDR_${addressUUID}_CREATE_RESPONSE", 'CREATED', 'text/plain')
                } else {
                    /* -- UPDATE -- */
                    logAttachment(messageLog, "ADDR_${addressUUID}_UPDATE_REQUEST", addrJson, 'application/json')
                    callUpdateAddress(baseUrl, user, pass, addrJson)
                    logAttachment(messageLog, "ADDR_${addressUUID}_UPDATE_RESPONSE", 'UPDATED', 'text/plain')
                }
            } /* --- Ende Address-Loop --- */
        }     /* --- Ende BP-Loop --- */

    } catch (Exception e) {
        handleError(message.getBody(String), e, messageLog)
    }

    return message
}

/* ============================================================
 * Funktion: Header & Properties setzen
 * ========================================================== */
void setHeadersAndProperties(Message msg, def messageLog) {

    /* --- Hilfsclosure zum Setzen mit Default -------------- */
    def setWithDefault = { String key ->
        if (msg.getProperty(key) == null) { msg.setProperty(key, 'placeholder') }
    }

    ['requestUser',
     'requestPassword',
     'requestURL',
     'RecipientBusinessSystemID_config',
     'RecipientBusinessSystemID_payload',
     'SenderBusinessSystemID'
    ].each { setWithDefault(it) }

    /* --- SenderSystemID ggf. aus XML übernehmen ----------- */
    def xmlBody = msg.getBody(String)
    if (xmlBody) {
        def tmpSender = new XmlSlurper().parseText(xmlBody).MessageHeader.SenderBusinessSystemID.text()
        if (tmpSender) { msg.setProperty('SenderBusinessSystemID', tmpSender) }
    }
}

/* ============================================================
 * Funktion: Business Partner Mapping  (XML ➜ JSON)
 * ========================================================== */
String buildBusinessPartnerPayload(def bp, Message msg) {

    /* --- Name ermitteln ---------------------------------- */
    String given      = bp.Common.Person.Name.GivenName.text()
    String family     = bp.Common.Person.Name.FamilyName.text()
    String org1       = bp.Common.Organisation.Name.FirstLineName.text()
    String supplierNm = family ? "${given ?: ''}${family}" : org1

    msg.setProperty('SupplierName', supplierNm)   // für Adressen-Mapping

    /* --- isActive (DeletedIndicator negieren) ------------- */
    def deleted = (bp.Common.DeletedIndicator.text() ?: bp.DeletedIndicator.text()) ?: 'false'
    String isActiveStr = (!deleted.equalsIgnoreCase('true')).toString()

    /* --- syncRequired Array ------------------------------- */
    def roles = bp.Role*.RoleCode*.text().findAll { it }
    if (!roles) { roles = ['Yes'] }  // Fallback gem. Beispiel-Output

    /* --- JSON aufbauen ------------------------------------ */
    def json = new JsonBuilder()
    json {
        Data {
            ICMExternalId          bp.InternalID.text()
            Name                   supplierNm
            ICMExternalSourceSystem msg.getProperty('SenderBusinessSystemID')
            isActive               isActiveStr
            syncRequired           roles
        }
    }
    return json.toPrettyString()
}

/* ============================================================
 * Funktion: Address Mapping  (XML ➜ JSON)
 * ========================================================== */
String buildAddressPayload(def addr, Message msg) {

    def json = new JsonBuilder()
    json {
        Data {
            ICMExternalId  addr.UUID.text()
            Name           msg.getProperty('SupplierName')
            ICMAddressLine1 addr.Address.PostalAddress.HouseID.text()
            ICMExternalSourceSystem msg.getProperty('SenderBusinessSystemID')
            ICMCountryCode { DisplayValue 'DE' }
            ICMICISupplierCode { DisplayValue msg.getProperty('SupplierCode') }
        }
    }
    return json.toPrettyString()
}

/* ============================================================
 * Funktion:  HTTP-Hilfsmethoden (GET/POST)
 * ========================================================== */
String sendHttpRequest(String urlStr,
                       String method,
                       String user,
                       String pass,
                       String body = '',
                       String cType = 'application/json') {

    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod(method)
    conn.setDoInput(true)
    conn.setRequestProperty('Authorization', 'Basic ' + "${user}:${pass}".bytes.encodeBase64().toString())

    if (method.equalsIgnoreCase('POST')) {
        conn.setDoOutput(true)
        conn.setRequestProperty('Content-Type', cType)
        conn.outputStream.withWriter('UTF-8') { it << (body ?: '') }
    }

    int rc = conn.responseCode
    InputStream is = rc >= 200 && rc < 400 ? conn.inputStream : conn.errorStream
    def resp = is ? is.getText('UTF-8') : ''

    if (rc >= 400) {
        throw new Exception("HTTP Fehler ${rc} bei Aufruf ${urlStr}: ${resp}")
    }
    return resp
}

/* ---------- API-Funktions­Wrapper ------------------------- */
String callCheckBusinessPartnerExists(String url, String sc, String u, String p) {
    sendHttpRequest("${url}?${sc}", 'GET', u, p)
}
void   callCreateBusinessPartner(String url, String u, String p, String payload) {
    sendHttpRequest("${url}/Create", 'POST', u, p, payload)
}
void   callUpdateBusinessPartner(String url, String u, String p, String payload) {
    sendHttpRequest("${url}/Update", 'POST', u, p, payload)
}
void   callSetActiveStatus(String url, String sc, String u, String p) {
    sendHttpRequest("${url}/${sc}/Activate", 'POST', u, p, '')
}

String callCheckAddressExists(String url, String uuid, String u, String p) {
    sendHttpRequest("${url}?${uuid}", 'GET', u, p)
}
void   callCreateAddress(String url, String u, String p, String payload) {
    sendHttpRequest("${url}/CreateAddress", 'POST', u, p, payload)
}
void   callUpdateAddress(String url, String u, String p, String payload) {
    sendHttpRequest("${url}/UpdateAddress", 'POST', u, p, payload)
}

/* ============================================================
 * Funktion: Logging – Payload als Attachment
 * ========================================================== */
void logAttachment(def messageLog, String name, String content, String type) {
    messageLog?.addAttachmentAsString(name, content ?: '', type ?: 'text/plain')
}

/* ============================================================
 * Funktion: Zentrales Error-Handling
 * ========================================================== */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def err = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(err, e)
}