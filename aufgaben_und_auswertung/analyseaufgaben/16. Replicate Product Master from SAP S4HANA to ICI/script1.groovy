/***************************************************************************************************
 * Groovy-Script  :   S4H-ICI_Product_Replication.groovy
 * Beschreibung   :   Vollständige Umsetzung der in der Aufgabenstellung beschriebenen Logik.
 * Autor          :   AI-Assistant
 **************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.*
import groovy.json.*
import java.nio.charset.StandardCharsets
import java.util.Base64

Message processData(Message message) {
    // Initiale Objekte
    def body        = message.getBody(String) ?: ""
    def messageLog  = messageLogFactory.getMessageLog(message)

    try {
        addLogAttachment(messageLog, "IncomingPayload", body)

        // Header & Property-Handling
        setHeaderAndPropertyDefaults(message)

        // XML parsen
        def xml = new XmlSlurper().parseText(body)

        // Produkte durchlaufen und validieren
        def validProducts = xml.'**'.findAll { it.name() == 'Product' && validateProduct(it) }
        if (validProducts.isEmpty()) {
            throw new RuntimeException("Kein gültiges Produkt gefunden (Produktnummer > 999 erforderlich).")
        }

        // Verarbeite jedes Produkt einzeln
        validProducts.each { prod ->
            processSingleProduct(prod, message, messageLog)
        }

    } catch (Exception e) {
        handleError(body, e, messageLog)  // Wirft RuntimeException weiter
    }

    return message
}

/* ================================================================================================
 * Hauptverarbeitung eines einzelnen Produktes
 * ==============================================================================================*/
def processSingleProduct(prod, Message message, def messageLog) {
    // Lokale Variablen
    def productId      = (prod.ProductInternalID.text() ?: "placeholder").trim()
    def productPayload = mapProduct(prod, message)
    addLogAttachment(messageLog, "ProductMapping_${productId}", productPayload)

    // Prüfe, ob Produkt existiert
    boolean productExists = callCheckProductExists(productId, message, messageLog)

    // CREATE oder UPDATE
    if (productExists) {
        callUpdateProduct(productPayload, message, messageLog)
    } else {
        callCreateProduct(productPayload, message, messageLog)
    }

    // Produkt aktiv setzen
    callSetActive(productId, message, messageLog)

    /************* Werke *************/
    def plants = prod.Plant.findAll { it.PlantID.text() }
    plants.each { pl ->
        processSinglePlant(pl, productId, message, messageLog)
    }
}

/* ================================================================================================
 * Hauptverarbeitung eines einzelnen Werks
 * ==============================================================================================*/
def processSinglePlant(plantNode, String productId, Message message, def messageLog) {
    def plantId      = (plantNode.PlantID.text() ?: "placeholder").trim()
    def plantPayload = mapPlant(plantNode, productId)
    addLogAttachment(messageLog, "PlantMapping_${productId}_${plantId}", plantPayload)

    boolean plantExists = callCheckPlantExists(plantId, message, messageLog)

    if (plantExists) {
        callUpdatePlant(plantPayload, message, messageLog)
    } else {
        callCreatePlant(plantPayload, message, messageLog)
    }
}

/* ================================================================================================
 * Header & Property-Handling
 * ==============================================================================================*/
def setHeaderAndPropertyDefaults(Message message) {
    // Headers
    def headers = message.getHeaders()
    headers.each { k,v -> message.setHeader(k, v) }       // Nur sicherstellen, dass alle vorhanden sind

    // Properties – falls nicht gesetzt => placeholder
    def props = message.getProperties()

    message.setProperty('requestUser'                 , props.get('requestUser')                 ?: 'placeholder')
    message.setProperty('requestPassword'             , props.get('requestPassword')             ?: 'placeholder')
    message.setProperty('requestURL'                  , props.get('requestURL')                  ?: 'placeholder')

    // Werte aus Payload
    def xml = new XmlSlurper().parseText(message.getBody(String))

    message.setProperty('SenderBusinessSystemID'      ,
            props.get('SenderBusinessSystemID')      ?: (xml.'**'.find { it.name() == 'SenderBusinessSystemID' }?.text() ?: 'placeholder'))

    message.setProperty('RecipientBusinessSystemID_payload',
            props.get('RecipientBusinessSystemID_payload') ?: (xml.'**'.find { it.name() == 'RecipientBusinessSystemID' }?.text() ?: 'placeholder'))

    message.setProperty('RecipientBusinessSystemID_config', 'Icertis_BUY')
}

/* ================================================================================================
 * Validierung eines Produktes
 * ==============================================================================================*/
boolean validateProduct(prod) {
    def id = prod.ProductInternalID.text()
    if (!id) { return false }
    def numberPart = (id =~ /(\d+)/)?.find()?.toInteger() ?: 0
    return numberPart > 999
}

/* ================================================================================================
 * Mapping-Funktionen
 * ==============================================================================================*/
String mapProduct(prod, Message message) {
    def senderSystem = message.getProperty('SenderBusinessSystemID')
    def productJson  = [
            Data:[
                ICMProductCode            : prod.ProductInternalID.text(),
                ICMProductTypeExternalID  : [DisplayValue: transformTypeCode(prod.ProductTypeCode.text())],
                ICMProductGroupCode       : [DisplayValue: prod.ProductGroupInternalID.text()],
                ICMExternalSourceSystem   : senderSystem,
                isActive                  : negateBoolean(prod.DeletedIndicator.text())
            ]
    ]
    return JsonOutput.prettyPrint(JsonOutput.toJson(productJson))
}

String mapPlant(plantNode, String productId) {
    def plantJson = [
            Data:[
                ICMPlantID  : [DisplayValue: plantNode.PlantID.text()],
                ICMExternalId:[DisplayValue: productId]
            ]
    ]
    return JsonOutput.prettyPrint(JsonOutput.toJson(plantJson))
}

/* ================================================================================================
 * Transformation-Hilfsfunktionen
 * ==============================================================================================*/
String transformTypeCode(String code) {
    switch (code?.toUpperCase()) {
        case 'FG' : return 'FinishedGood'
        case 'HALB': return 'SemiFinishedGood'
        default   : return code ?: 'Unknown'
    }
}

String negateBoolean(String value) {
    if (value?.toLowerCase() == 'true')  { return "false" }
    if (value?.toLowerCase() == 'false') { return "true"  }
    return "true"   // Default: aktiv
}

/* ================================================================================================
 * API-CALLS
 * ==============================================================================================*/
boolean callCheckProductExists(String productId, Message message, def messageLog) {
    def url = "${message.getProperty('requestURL')}?${URLEncoder.encode(productId,'UTF-8')}"
    addLogAttachment(messageLog, "CheckProductExists_Request_${productId}", url)
    def conn = openConnection(url, message, 'GET')
    def response = getResponse(conn)
    addLogAttachment(messageLog, "CheckProductExists_Response_${productId}", response)
    return response?.contains('"PRODUCT"')
}

void callCreateProduct(String payload, Message message, def messageLog) {
    def url = "${message.getProperty('requestURL')}/Create"
    executePost(url, payload, message, messageLog, "CreateProduct")
}

void callUpdateProduct(String payload, Message message, def messageLog) {
    def url = "${message.getProperty('requestURL')}/Update"
    executePost(url, payload, message, messageLog, "UpdateProduct")
}

void callSetActive(String productId, Message message, def messageLog) {
    def url = "${message.getProperty('requestURL')}/${URLEncoder.encode(productId,'UTF-8')}/Activate"
    executePost(url, "", message, messageLog, "SetActive")
}

boolean callCheckPlantExists(String plantId, Message message, def messageLog) {
    def url = "${message.getProperty('requestURL')}?${URLEncoder.encode(plantId,'UTF-8')}"
    addLogAttachment(messageLog, "CheckPlantExists_Request_${plantId}", url)
    def conn = openConnection(url, message, 'GET')
    def response = getResponse(conn)
    addLogAttachment(messageLog, "CheckPlantExists_Response_${plantId}", response)
    return response?.contains('"PLANT"')
}

void callCreatePlant(String payload, Message message, def messageLog) {
    def url = "${message.getProperty('requestURL')}/CreatePlant"
    executePost(url, payload, message, messageLog, "CreatePlant")
}

void callUpdatePlant(String payload, Message message, def messageLog) {
    def url = "${message.getProperty('requestURL')}/UpdatePlant"
    executePost(url, payload, message, messageLog, "UpdatePlant")
}

/* ================================================================================================
 * HTTP-Hilfsfunktionen
 * ==============================================================================================*/
HttpURLConnection openConnection(String url, Message message, String method) {
    def conn = (HttpURLConnection) new URL(url).openConnection()
    conn.setRequestMethod(method)
    conn.setDoOutput(method == 'POST')
    conn.setRequestProperty('Authorization', createBasicAuth(message))
    conn.setRequestProperty('Content-Type', 'application/json')
    conn
}

String createBasicAuth(Message message) {
    def user = message.getProperty('requestUser')
    def pass = message.getProperty('requestPassword')
    return "Basic " + Base64.encoder.encodeToString("${user}:${pass}".getBytes(StandardCharsets.UTF_8))
}

String getResponse(HttpURLConnection conn) {
    def isr = (conn.responseCode >= 200 && conn.responseCode < 300) ? conn.inputStream : conn.errorStream
    return isr?.getText('UTF-8')
}

void executePost(String url, String payload, Message message, def messageLog, String tag) {
    addLogAttachment(messageLog, "${tag}_Request_${url.tokenize('/')[-1]}", payload)
    def conn = openConnection(url, message, 'POST')
    if (payload) {
        conn.outputStream.withWriter { it << payload }
    }
    def response = getResponse(conn)
    addLogAttachment(messageLog, "${tag}_Response_${url.tokenize('/')[-1]}", response ?: "")
}

/* ================================================================================================
 * Gemeinsames Logging
 * ==============================================================================================*/
void addLogAttachment(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content ?: "", "text/plain")
}

/* ================================================================================================
 * Error-Handling
 * ==============================================================================================*/
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im Processing-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}