/******************************************************************************************
 *  Business Partner Integration – S/4 HANA → ICI (Sell Side)
 *  -----------------------------------------------------------------
 *  • Modular aufgebaut (Funktionen am Ende der Datei)
 *  • Kommentiert in Deutsch
 *  • Umfangreiches Error-Handling und Logging
 *  • Mapping nach Vorgabe (XML → JSON)
 *  • HTTP-Aufrufe für Existence-Check, Create, Update und Activate
 ******************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.nio.charset.StandardCharsets
import java.util.Base64

Message processData(Message message) {

    // 1) Vorbereitungen & Auslesen des Original-Payloads
    def messageLog  = messageLogFactory.getMessageLog(message)
    def originalXml = message.getBody(String) ?: ''
    logPayload(messageLog, 'OriginalPayload', originalXml, 'text/xml')

    // 2) Header/Property-Handling
    def meta = setHeadersAndProperties(message, originalXml)

    // 3) Business-Partner aus dem XML extrahieren
    def partnerNodes = extractBusinessPartners(originalXml)
    def validPartnerNodes = partnerNodes.findAll { isValidPartner(it) }

    if (!validPartnerNodes) {
        handleError(originalXml, new Exception('Keine gültigen Business-Partner (InternalID > 999) gefunden.'), messageLog)
    }

    // 4) Iterative Verarbeitung aller gültigen Business-Partner
    validPartnerNodes.each { bpNode ->

        // SupplierCode für diesen Partner setzen
        def supplierCode = (bpNode.InternalID.text() ?: '').trim()
        message.setProperty('SupplierCode', supplierCode)
        meta.SupplierCode = supplierCode

        // 5) Existenz-Prüfung (GET)
        def exists = checkIfExists(meta, messageLog)

        // 6) Mapping (XML → JSON)
        def jsonPayload = mapBusinessPartner(bpNode, messageLog)

        // 7) Create / Update je nach Existenz
        if (exists) {
            callUpdate(meta, jsonPayload, messageLog)
        } else {
            callCreate(meta, jsonPayload, messageLog)
        }

        // 8) Aktivierung
        callActivate(meta, messageLog)
    }

    // 9) Rückgabe unveränderter Message (Payload bleibt das ursprüngliche XML)
    return message
}

/* ========================================================================================
 *                                     FUNKTIONEN
 * ======================================================================================*/

/* --------------------------------------------------
 *  Fehlerbehandlung – wirft RuntimeException
 * --------------------------------------------------*/
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

/* --------------------------------------------------
 *  Logging – legt Payload als Attachment ab
 * --------------------------------------------------*/
def logPayload(def messageLog, String name, String payload, String type = 'text/plain') {
    try {
        messageLog?.addAttachmentAsString(name, payload, type)
    } catch (Exception ignored) {
        // Silent fail – kein Abbruch wegen Logging
    }
}

/* --------------------------------------------------
 *  Header & Property – Ermittlung/Setzen
 * --------------------------------------------------*/
def setHeadersAndProperties(Message message, String xmlBody) {
    def slurper = new XmlSlurper().parseText(xmlBody)
    def sender    = slurper?.MessageHeader?.SenderBusinessSystemID?.text()     ?: 'placeholder'
    def recipient = slurper?.MessageHeader?.RecipientBusinessSystemID?.text() ?: 'placeholder'

    // bestehende Werte vorrangig nutzen, sonst default „placeholder“
    def props = [
        requestUser                    : message.getProperty('requestUser')                    ?: 'placeholder',
        requestPassword                : message.getProperty('requestPassword')                ?: 'placeholder',
        requestURL                     : message.getProperty('requestURL')                     ?: 'placeholder',
        RecipientBusinessSystemID_config : 'Icertis_SELL',
        RecipientBusinessSystemID_payload: recipient,
        SenderBusinessSystemID         : sender,
        SupplierCode                   : 'placeholder'
    ]

    // in der Message verfügbar machen
    props.each { k, v -> message.setProperty(k, v) }
    return props
}

/* --------------------------------------------------
 *  XML → Liste Business-Partner Nodes
 * --------------------------------------------------*/
def extractBusinessPartners(String xmlBody) {
    def root = new XmlSlurper().parseText(xmlBody)
    return root?.BusinessPartner ?: []
}

/* --------------------------------------------------
 *  Validierung InternalID > 999
 * --------------------------------------------------*/
def isValidPartner(def bpNode) {
    def id = bpNode?.InternalID?.text()
    if (!id) { return false }
    try {
        // Nicht-numerische Zeichen ignorieren (BP12345 → 12345)
        def numeric = id.replaceAll(/[^0-9]/, '')
        return numeric?.toLong() > 999
    } catch (Exception ignored) {
        return false
    }
}

/* --------------------------------------------------
 *  Mapping Business-Partner (XML → JSON-String)
 * --------------------------------------------------*/
def mapBusinessPartner(def bpNode, def messageLog) {

    // Vor-Mapping-Logging
    logPayload(messageLog, 'BeforeMapping_BP', groovy.xml.XmlUtil.serialize(bpNode), 'text/xml')

    // Quelldaten
    def naturalPerson = (bpNode?.Common?.NaturalPersonIndicator?.text() ?: 'false').toBoolean()
    def internalId    = bpNode?.InternalID?.text()            ?: ''
    def customerId    = bpNode?.Customer?.InternalID?.text() ?: ''
    def givenName     = bpNode?.Common?.Person?.Name?.GivenName?.text()           ?: ''
    def familyName    = bpNode?.Common?.Person?.Name?.FamilyName?.text()          ?: ''
    def addFamilyName = bpNode?.Common?.Person?.Name?.AdditionalFamilyName?.text()?: ''
    def orgName       = bpNode?.Common?.Organisation?.Name?.FirstLineName?.text() ?: ''
    def roleCodes     = bpNode?.Role*.RoleCode*.text() ?: []
    def countryCode   = bpNode?.AddressInformation?.Address?.PostalAddress?.CountryCode?.text() ?: ''

    // Mapping laut Vorgabe
    def contractPartyType = naturalPerson ? 'Individual' : 'Customer'
    def externalId        = naturalPerson ? customerId   : internalId
    def name
    if (naturalPerson) {
        name = [givenName, familyName, addFamilyName]
                .findAll { it }                             // Null/Empty entfernen
                .join(' ')
    } else {
        name = orgName
    }

    def jsonMap = [
        Data: [
            ICMContractingPartyType: contractPartyType,
            ICMExternalId         : externalId,
            Name                  : name,
            syncRequired          : roleCodes,              // Liste → JSONArray
            ICMCountryCode        : [
                DisplayValue: countryCode
            ]
        ]
    ]

    def jsonString = new JsonBuilder(jsonMap).toPrettyString()

    // Nach-Mapping-Logging
    logPayload(messageLog, 'AfterMapping_BP', jsonString, 'application/json')
    return jsonString
}

/* --------------------------------------------------
 *  Basis – Authorization Header (Basic)
 * --------------------------------------------------*/
def buildAuthHeader(Map meta) {
    def userPass = "${meta.requestUser}:${meta.requestPassword}"
    return 'Basic ' + Base64.encoder.encodeToString(userPass.getBytes(StandardCharsets.UTF_8))
}

/* --------------------------------------------------
 *  HTTP-Helfer – führt Request aus und gibt Response-Body zurück
 * --------------------------------------------------*/
def executeHttp(String method, String urlStr, String authHeader, String body, String attachmentName, def messageLog) {

    logPayload(messageLog, "Request_$attachmentName", body ?: '', (method == 'GET') ? 'text/plain' : 'application/json')

    HttpURLConnection conn = null
    try {
        conn = (HttpURLConnection) new URL(urlStr).openConnection()
        conn.setRequestMethod(method)
        conn.setRequestProperty('Authorization', authHeader)
        conn.setRequestProperty('Content-Type', 'application/json')
        conn.setDoInput(true)
        if (body) {
            conn.setDoOutput(true)
            conn.outputStream.withWriter('UTF-8') { it << body }
        }

        def responseText = conn.inputStream.getText('UTF-8')
        logPayload(messageLog, "Response_$attachmentName", responseText ?: '', 'application/json')
        return [code: conn.responseCode, body: responseText]

    } catch (Exception e) {
        // Fehlerresponse (z.B. 4xx/5xx)
        def errorStream = conn?.errorStream?.getText('UTF-8')
        logPayload(messageLog, "Response_$attachmentName", errorStream ?: e.message, 'text/plain')
        throw e
    } finally {
        conn?.disconnect()
    }
}

/* --------------------------------------------------
 *  API-CALL – Existenz-Check (GET)
 * --------------------------------------------------*/
def checkIfExists(Map meta, def messageLog) {
    def url = "${meta.requestURL}?${meta.SupplierCode}"
    def authHeader = buildAuthHeader(meta)
    def resp = executeHttp('GET', url, authHeader, null, 'CheckExists', messageLog)
    // wenn Response-Body ein <BusinessPartner>-Tag enthält → existiert
    return resp.body?.contains('<BusinessPartner>')
}

/* --------------------------------------------------
 *  API-CALL – CREATE
 * --------------------------------------------------*/
def callCreate(Map meta, String jsonPayload, def messageLog) {
    def url = "${meta.requestURL}/Create"
    def authHeader = buildAuthHeader(meta)
    executeHttp('POST', url, authHeader, jsonPayload, 'Create', messageLog)
}

/* --------------------------------------------------
 *  API-CALL – UPDATE
 * --------------------------------------------------*/
def callUpdate(Map meta, String jsonPayload, def messageLog) {
    def url = "${meta.requestURL}/Update"
    def authHeader = buildAuthHeader(meta)
    executeHttp('POST', url, authHeader, jsonPayload, 'Update', messageLog)
}

/* --------------------------------------------------
 *  API-CALL – ACTIVATE
 * --------------------------------------------------*/
def callActivate(Map meta, def messageLog) {
    def url = "${meta.requestURL}/${meta.SupplierCode}/Activate"
    def authHeader = buildAuthHeader(meta)
    executeHttp('POST', url, authHeader, '', 'Activate', messageLog)
}