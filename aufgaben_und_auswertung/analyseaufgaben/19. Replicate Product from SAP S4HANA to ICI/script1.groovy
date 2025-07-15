import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput
import java.net.URL
import java.net.HttpURLConnection
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

Message processData(Message message) {

    def messageLog   = messageLogFactory.getMessageLog(message)
    String origBody  = message.getBody(String) ?: ''

    try {

        // 1. Header & Properties setzen
        setContextValues(message, origBody, messageLog)

        // 2. XML parsen & validieren
        def productXml = new XmlSlurper().parseText(origBody)
        validateProduct(productXml, origBody, messageLog)

        // 3. Mapping durchführen
        logPayload(messageLog, 'Payload_vor_Mapping', origBody)
        String jsonPayload = mapProductToICIJson(productXml)
        logPayload(messageLog, 'Payload_nach_Mapping', jsonPayload)

        // 4. Existenz prüfen
        String reqURL   = message.getProperty('requestURL')
        String user     = message.getProperty('requestUser')
        String pwd      = message.getProperty('requestPassword')
        String prodID   = message.getProperty('ProductInternalID')

        String operation = checkProductExists(reqURL, user, pwd, prodID, messageLog)

        // 5. CREATE oder UPDATE ausführen
        if ('UPDATE' == operation) {
            callUpdateProduct(reqURL, user, pwd, jsonPayload, messageLog)
        } else {
            callCreateProduct(reqURL, user, pwd, jsonPayload, messageLog)
        }

        // 6. Aktiv-Status setzen
        callActivateProduct(reqURL, user, pwd, prodID, jsonPayload, messageLog)

        // 7. Body weiterreichen
        message.setBody(jsonPayload)
        return message

    } catch (Exception e) {
        handleError(origBody, e, messageLog)
    }
}

/* =======================================================================
 * Funktions­definitionen
 * =======================================================================
 */

/** Setzt notwendige Header & Properties, verwendet 'placeholder' wenn Wert fehlt */
def setContextValues(Message msg, String payload, def log) {

    def props   = msg.getProperties()
    def headers = msg.getHeaders()
    def xml
    try { xml = new XmlSlurper().parseText(payload) } catch (ignored) {}

    def val = { map, key -> map?.containsKey(key) ? map[key] : 'placeholder' }

    msg.setProperty('requestUser' , val(props,'requestUser'))
    msg.setProperty('requestPassword', val(props,'requestPassword'))
    msg.setProperty('requestURL', val(props,'requestURL'))

    String sender    = xml?.depthFirst()?.find{ it.name()=='SenderBusinessSystemID' }?.text() ?: 'placeholder'
    String recipient = xml?.depthFirst()?.find{ it.name()=='RecipientBusinessSystemID' }?.text() ?: 'placeholder'
    String prodID    = xml?.depthFirst()?.find{ it.name()=='ProductInternalID' }?.text() ?: 'placeholder'
    String prodGrp   = xml?.depthFirst()?.find{ it.name()=='ProductGroupInternalID' }?.text() ?: 'placeholder'

    msg.setProperty('SenderBusinessSystemID', sender)
    msg.setProperty('RecipientBusinessSystemID_payload', recipient)
    msg.setProperty('RecipientBusinessSystemID_config', 'Icertis_SELL')
    msg.setProperty('ProductInternalID', prodID)
    msg.setProperty('ProductGroupInternalID', prodGrp)
}

/** Validiert die Produktnummer (> 999) */
def validateProduct(prodNode, String body, def log) {
    String id = prodNode?.ProductInternalID?.text() ?: ''
    def m = id =~ /(\d+)/
    if (!m) { throw new IllegalArgumentException("Keine Nummer in '${id}' gefunden") }
    if ((m[0][1] as long) <= 999) {
        throw new IllegalArgumentException("Produkt ${id} wird verworfen (Nummer <= 999)")
    }
}

/** XML -> JSON Mapping entsprechend Vorgabe */
String mapProductToICIJson(prod) {

    String internalId  = prod.ProductInternalID?.text()
    String typeCode    = prod.ProductTypeCode?.text()
    String descr       = prod.Description?.Description?.text()
    String groupId     = prod.ProductGroupInternalID?.text()
    String deleted     = prod.DeletedIndicator?.text()
    String isActiveStr = (!'true'.equalsIgnoreCase(deleted)).toString()

    def j = [
        Data:[
            ICMProductName        : descr,
            ICMProductType        : typeCode,
            ICMProductCode        : internalId,
            isActive              : isActiveStr,
            ICMProductCategoryName:[
                DisplayValue: groupId
            ]
        ]
    ]
    JsonOutput.toJson(j)
}

/** Prüft ob das Produkt existiert (GET). Liefert 'CREATE' oder 'UPDATE' zurück */
String checkProductExists(String baseUrl, String user, String pwd, String prodId, def log) {

    String url = "${baseUrl}?${URLEncoder.encode(prodId, StandardCharsets.UTF_8.name())}"
    logPayload(log, 'CheckProductExist_Request', url)

    HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection()
    con.requestMethod      = 'GET'
    con.connectTimeout     = 30000
    con.readTimeout        = 30000
    con.setRequestProperty('Authorization', basicAuth(user, pwd))

    int rc        = con.responseCode
    String resp   = con.inputStream?.getText('UTF-8') ?: ''
    logPayload(log, 'CheckProductExist_Response', "HTTP ${rc}\n${resp}")

    (rc == 200 && resp?.trim()) ? 'UPDATE' : 'CREATE'
}

/** POST-Aufruf UPDATE */
def callUpdateProduct(String baseUrl, String user, String pwd, String payload, def log) {
    callPost("${baseUrl}/Update", user, pwd, payload, 'UpdateProduct', log)
}

/** POST-Aufruf CREATE */
def callCreateProduct(String baseUrl, String user, String pwd, String payload, def log) {
    callPost("${baseUrl}/Create", user, pwd, payload, 'CreateProduct', log)
}

/** POST-Aufruf Aktiv setzen */
def callActivateProduct(String baseUrl, String user, String pwd, String prodId, String payload, def log) {
    String url = "${baseUrl}/${URLEncoder.encode(prodId, StandardCharsets.UTF_8.name())}/Activate"
    callPost(url, user, pwd, payload, 'ActivateProduct', log)
}

/** Generische POST-Funktion mit Logging */
def callPost(String url, String user, String pwd, String payload, String logPrefix, def log) {

    logPayload(log, "${logPrefix}_Request", payload)

    HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection()
    con.requestMethod      = 'POST'
    con.doOutput           = true
    con.connectTimeout     = 30000
    con.readTimeout        = 30000
    con.setRequestProperty('Content-Type', 'application/json')
    con.setRequestProperty('Authorization', basicAuth(user, pwd))
    con.outputStream.withWriter('UTF-8') { it << payload }

    int rc      = con.responseCode
    String resp = con.inputStream?.getText('UTF-8') ?: ''
    logPayload(log, "${logPrefix}_Response", "HTTP ${rc}\n${resp}")
}

/** Erstellt Basic-Auth Header */
String basicAuth(String user, String pwd) {
    'Basic ' + "${user}:${pwd}".bytes.encodeBase64().toString()
}

/** MPL-Attachment Logging */
def logPayload(def messageLog, String name, String payload) {
    messageLog?.addAttachmentAsString(name, payload, 'text/plain')
}

/** Zentrales Fehler-Handling */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    throw new RuntimeException("Fehler im Skript: ${e.message}", e)
}