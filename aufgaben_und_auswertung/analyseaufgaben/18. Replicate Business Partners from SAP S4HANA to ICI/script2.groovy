/*****************************************************************************************
 *  Groovy-Skript:  Business Partner – S/4 → ICI (Sell Side)
 *
 *  Beschreibung
 *  ============
 *  ·  Liest eingehenden Business-Partner-Bulk-Request
 *  ·  Validiert gemäß Vorgabe (InternalID   >  999)
 *  ·  Ruft ICI-API (EXISTS, CREATE / UPDATE, ACTIVATE) für jeden Partner auf
 *  ·  Erstellt erforderliches JSON-Mapping
 *  ·  Schreibt alle relevanten Payloads als Attachment in das Message-Monitoring
 *  ·  Sämtliche Funktionen sind strikt modular aufgebaut
 *
 *  Hinweise
 *  ========
 *  ·  Keine globalen Variablen oder Konstanten (gem. Vorgabe)
 *  ·  XmlSlurper ist bereits verfügbar, daher kein Import nötig
 *  ·  Unbenutzte Imports wurden vermieden
 *****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.nio.charset.StandardCharsets

/********************************* Error-Handling ***************************************/
def handleError(String originalBody, Exception e, def messageLog) {
    /*  Wirft eine RuntimeException, nachdem der fehlerhafte Payload als Attachment
        gespeichert und eine aussagekräftige Meldung geloggt wurde.                */
    messageLog?.addAttachmentAsString('ErrorPayload', originalBody, 'text/xml')
    def errorMsg = "Fehler im BP-Skript: ${e.message}"
    messageLog?.setStringProperty('GroovyError', errorMsg)
    throw new RuntimeException(errorMsg, e)
}

/*********************************** Logging ********************************************/
def addLogAttachment(def messageLog, String name, String content, String mime = 'text/plain') {
    /*  Zentrale Logging-Funktion.                                                     */
    messageLog?.addAttachmentAsString(name, content, mime)
}

/*************************** Property / Header Handling *********************************/
def prepareContext(Message message, def xmlRoot) {
    /*  Setzt alle benötigten Header und Properties; bereits vorhandene Werte werden
        wiederverwendet, andernfalls mit „placeholder“ initialisiert.                 */
    def props     = message.getProperties()
    def hdrs      = message.getHeaders()
    def getVal    = { container, key -> container.containsKey(key) ? container[key] : 'placeholder' }

    // Header / Property-Mapping
    message.setProperty('requestUser'                 , getVal(props , 'requestUser'))
    message.setProperty('requestPassword'             , getVal(props , 'requestPassword'))
    message.setProperty('requestURL'                  , getVal(props , 'requestURL'))
    message.setProperty('RecipientBusinessSystemID_config' , 'Icertis_SELL')

    // Werte aus dem eingehenden XML (sofern vorhanden)
    message.setProperty('RecipientBusinessSystemID_payload',
            xmlRoot?.MessageHeader?.RecipientBusinessSystemID?.text() ?: 'placeholder')
    message.setProperty('SenderBusinessSystemID',
            xmlRoot?.MessageHeader?.SenderBusinessSystemID?.text()    ?: 'placeholder')

    return message
}

/*********************************** Mapping ********************************************/
def mapBusinessPartner(def bpNode) {
    /*  Erstellt das Ziel-JSON gem. Mapping-Spezifikation.                             */

    boolean isNatural = bpNode.Common?.NaturalPersonIndicator?.text()?.toBoolean()

    // ICMContractingPartyType
    String contractingPartyType = isNatural ? 'Customer (Person)' : 'Customer'   // siehe Spezifikation

    // ICMExternalId
    String icmExternalId = isNatural
            ? bpNode.Customer?.InternalID?.text()
            : bpNode.InternalID?.text()

    // Name
    String name = isNatural
            ? [
                    bpNode.Common?.Person?.Name?.GivenName?.text(),
                    bpNode.Common?.Person?.Name?.FamilyName?.text(),
                    bpNode.Common?.Person?.Name?.AdditionalFamilyName?.text()
              ].findAll { it }                                    // entfernt null / leere Elemente
               .join(' ')
            : bpNode.Common?.Organisation?.Name?.FirstLineName?.text()

    // syncRequired
    def roleCodes = (bpNode.Role*.RoleCode*.text()).flatten().findAll { it }   // Liste aller RoleCodes

    // CountryCode
    String country = bpNode.AddressInformation
                         ?.Address
                         ?.PostalAddress
                         ?.CountryCode
                         ?.text()

    def json = [
            Data:[
                ICMContractingPartyType : contractingPartyType,
                ICMExternalId           : icmExternalId,
                Name                    : name,
                syncRequired            : roleCodes,
                ICMCountryCode          : [DisplayValue: country]
            ]
    ]

    return new JsonBuilder(json).toPrettyString()
}

/******************************** HTTP-Hilfsfunktionen **********************************/
def httpGet(String url, String user, String pwd) {
    /*  Führt einen GET-Request mit Basic-Auth aus und liefert Map(code, body).        */
    def conn = new URL(url).openConnection()
    conn.setRequestProperty('Authorization',
            'Basic ' + "${user}:${pwd}".bytes.encodeBase64().toString())
    conn.requestMethod = 'GET'
    conn.connect()
    def respBody = ''
    try { respBody = conn.inputStream.getText(StandardCharsets.UTF_8.name()) } catch (ignored) {}
    return [code: conn.responseCode, body: respBody]
}

def httpPost(String url, String user, String pwd, String jsonBody) {
    /*  Führt einen POST-Request mit Basic-Auth aus; Response-Body wird zurückgegeben. */
    byte[] postData = jsonBody.getBytes(StandardCharsets.UTF_8)
    def conn = new URL(url).openConnection()
    conn.with {
        doOutput       = true
        requestMethod  = 'POST'
        setRequestProperty('Authorization',
                'Basic ' + "${user}:${pwd}".bytes.encodeBase64().toString())
        setRequestProperty('Content-Type', 'application/json')
        setRequestProperty('Content-Length', "${postData.length}")
        outputStream.withStream { it.write(postData) }
    }
    def respBody = ''
    try { respBody = conn.inputStream.getText(StandardCharsets.UTF_8.name()) } catch (ignored) {}
    return [code: conn.responseCode, body: respBody]
}

/********************************* API-Calls ********************************************/
def callExists(String baseUrl, String supplierCode, String user, String pwd) {
    return httpGet("${baseUrl}?${supplierCode}", user, pwd)          // Query-String: SupplierCode
}

def callUpdate(String baseUrl, String jsonBody, String user, String pwd) {
    return httpPost("${baseUrl}/Update", user, pwd, jsonBody)
}

def callCreate(String baseUrl, String jsonBody, String user, String pwd) {
    return httpPost("${baseUrl}/Create", user, pwd, jsonBody)
}

def callActivate(String baseUrl, String supplierCode, String jsonBody,
                 String user, String pwd) {
    return httpPost("${baseUrl}/${supplierCode}/Activate", user, pwd, jsonBody)
}

/*********************************** MAIN ***********************************************/
Message processData(Message message) {
    def msgLog = messageLogFactory.getMessageLog(message)
    def originalBody = message.getBody(String) ?: ''
    addLogAttachment(msgLog, 'IncomingPayload', originalBody, 'text/xml')

    try {
        /************ Parsing & Kontext-Vorbereitung *********************************/
        def root  = new XmlSlurper().parseText(originalBody)
        prepareContext(message, root)

        /************ Business-Partner-Iteration *************************************/
        def partners = root.BusinessPartner
        // Validierung: mind. ein gültiger BP
        def validPartners = partners.findAll { bp ->
            try {
                // Prüfe numerischen Wert (alles außer Ziffern wird entfernt)
                def numPart = bp.InternalID?.text()?.replaceAll('[^0-9]', '')
                return numPart?.isInteger() && numPart.toInteger() > 999
            } catch (ignored) { return false }
        }

        if (!validPartners) {
            throw new IllegalStateException('Kein gültiger Business Partner (InternalID > 999) gefunden.')
        }

        // Benötigte Property-Werte (für alle API-Aufrufe identisch)
        String requestURL  = message.getProperty('requestURL')
        String requestUser = message.getProperty('requestUser')
        String requestPwd  = message.getProperty('requestPassword')

        validPartners.eachWithIndex { bp, idx ->
            /*---------------------------------------------------------------------*/
            /*   Split-Verarbeitung – jeder BP wird einzeln abgehandelt             */
            /*---------------------------------------------------------------------*/
            String supplierCode = bp.InternalID?.text()
            message.setProperty('SupplierCode', supplierCode)

            /************ Mapping **************************************************/
            String jsonRequest = mapBusinessPartner(bp)
            addLogAttachment(msgLog, "BP_${idx+1}_MappedRequest", jsonRequest, 'application/json')

            /************ EXISTS-Check *********************************************/
            addLogAttachment(msgLog, "BP_${idx+1}_Exists_Request", "GET ${requestURL}?${supplierCode}")
            def existsResp = callExists(requestURL, supplierCode, requestUser, requestPwd)
            addLogAttachment(msgLog, "BP_${idx+1}_Exists_Response",
                             "HTTP ${existsResp.code}\n${existsResp.body}")

            boolean exists = existsResp.body?.trim()
                               ?.contains('"BusinessPartner"')        // sehr einfache Heuristik
            /************ CREATE oder UPDATE **************************************/
            if (exists) {
                addLogAttachment(msgLog, "BP_${idx+1}_Update_Request", jsonRequest, 'application/json')
                def updResp = callUpdate(requestURL, jsonRequest, requestUser, requestPwd)
                addLogAttachment(msgLog, "BP_${idx+1}_Update_Response",
                                 "HTTP ${updResp.code}\n${updResp.body}")
            } else {
                addLogAttachment(msgLog, "BP_${idx+1}_Create_Request", jsonRequest, 'application/json')
                def crtResp = callCreate(requestURL, jsonRequest, requestUser, requestPwd)
                addLogAttachment(msgLog, "BP_${idx+1}_Create_Response",
                                 "HTTP ${crtResp.code}\n${crtResp.body}")
            }

            /************ ACTIVATE *************************************************/
            addLogAttachment(msgLog, "BP_${idx+1}_Activate_Request", jsonRequest, 'application/json')
            def actResp = callActivate(requestURL, supplierCode, jsonRequest, requestUser, requestPwd)
            addLogAttachment(msgLog, "BP_${idx+1}_Activate_Response",
                             "HTTP ${actResp.code}\n${actResp.body}")
        }

        /************ Abschluss ****************************************************/
        message.setBody('Business Partner Verarbeitung abgeschlossen.')
        return message

    } catch (Exception ex) {
        handleError(originalBody, ex, msgLog)      // wirft RuntimeException
    }
}