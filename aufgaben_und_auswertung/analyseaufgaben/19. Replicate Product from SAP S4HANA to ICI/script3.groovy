/***********************************************
*  Product-Master Integration – S4/HANA → ICI  *
*  Author:      ChatGPT (Senior Integration)   *
*  Created:     2025-06-18                     *
*  IFlow Step:  Groovy Script                  *
************************************************
*  Dieses Skript verarbeitet eine Bulk-Payload *
*  mit Produkt-Stammdaten, führt die            *
*  entsprechenden REST-Aufrufe (CHECK, CREATE/ *
*  UPDATE, ACTIVATE) gegen ICI durch und       *
*  erfüllt sämtliche in der Aufgabenstellung   *
*  genannten Anforderungen.                    *
************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.net.HttpURLConnection
import java.net.URLEncoder
import java.util.Base64

// =========================================================
// ===============    ENTRY POINT (process)   ===============
// =========================================================
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    String originalBody = message.getBody(String) ?: ''

    try {
        // 1. Headers & Properties
        setPropertiesAndHeaders(message, originalBody, messageLog)

        // 2. Payload sichern
        message.setProperty('originalPayload', originalBody)

        // 3. Produkte splitten & verarbeiten
        def xml = new XmlSlurper().parseText(originalBody)
        def products = xml.'**'.findAll { it.name() == 'Product' }

        // 4. Produkte nach Validierungsregel filtern
        def validProducts = products.findAll { isValidProduct(it.ProductInternalID.text()) }
        if (validProducts.isEmpty()) {
            throw new IllegalArgumentException('Kein gültiges Produkt (Nummer > 999) gefunden!')
        }

        // 5. Jedes Produkt einzeln verarbeiten
        validProducts.each { prod ->
            processSingleProduct(prod, message, messageLog)
        }

        // Rückgabe des (unveränderten) Original-Bodies
        message.setBody(originalBody)
        return message

    } catch (Exception e) {
        handleError(originalBody, e, messageLog)
        return message   // wird nie erreicht, handleError wirft Exception
    }
}

// =========================================================
// ===============     HELPER / UTILITIES     ===============
// =========================================================

/* Prüft, ob die Produktnummer den Regeln entspricht */
boolean isValidProduct(String productId) {
    def matcher = productId =~ /(\d+)/
    return (matcher ? matcher[0][1].toBigInteger() > 999 : false)
}

/* Erstellt Base64-Header für Basic-Auth */
String basicAuthHeader(String user, String pass) {
    Base64.encoder.encodeToString("${user}:${pass}".bytes)
}

/* Liest vorhandene Header/Properties oder liefert 'placeholder' */
String valueOrPlaceholder(def existing) {
    existing ? existing.toString() : 'placeholder'
}

/* Fügt einen Payload-Log als Attachment hinzu */
void logPayload(def messageLog, String name, String payload, String type = 'text/plain') {
    messageLog?.addAttachmentAsString(name, payload, type)
}

/* Globales Error-Handling */
void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

// =========================================================
// ===============     CORE FUNCTIONALITY     ===============
// =========================================================

/* Liest/Setzt Properties & Header */
void setPropertiesAndHeaders(Message msg, String body, def messageLog) {
    def xml = new XmlSlurper().parseText(body)

    // Payload-basierte Werte
    String senderID     = xml.'**'.find { it.name() == 'SenderBusinessSystemID' }?.text()
    String recipientID  = xml.'**'.find { it.name() == 'RecipientBusinessSystemID' }?.text()

    // Standard-Properties
    msg.setProperty('requestUser',                valueOrPlaceholder(msg.getProperty('requestUser')))
    msg.setProperty('requestPassword',            valueOrPlaceholder(msg.getProperty('requestPassword')))
    msg.setProperty('requestURL',                 valueOrPlaceholder(msg.getProperty('requestURL')))
    msg.setProperty('RecipientBusinessSystemID_config', 'Icertis_SELL')
    msg.setProperty('RecipientBusinessSystemID_payload', recipientID ?: 'placeholder')
    msg.setProperty('SenderBusinessSystemID',     senderID ?: 'placeholder')
}

/* Verarbeitet ein einzelnes Produkt */
void processSingleProduct(def product, Message message, def messageLog) {

    // Produkt-spezifische Properties setzen
    String productId  = product.ProductInternalID.text()
    String groupId    = product.ProductGroupInternalID.text()
    message.setProperty('ProductInternalID',     productId)
    message.setProperty('ProductGroupInternalID', groupId)

    // ---------- Mapping ----------
    logPayload(messageLog, "PreMapping_${productId}", groovy.xml.XmlUtil.serialize(product), 'text/xml')
    String jsonPayload = mapProductToJson(product)
    logPayload(messageLog, "PostMapping_${productId}", jsonPayload, 'application/json')

    // ---------- Credentials & URL ----------
    String baseURL   = message.getProperty('requestURL')
    String user      = message.getProperty('requestUser')
    String pass      = message.getProperty('requestPassword')
    String auth      = basicAuthHeader(user, pass)

    // ---------- Check Exists ----------
    boolean exists = callProductExists(baseURL, productId, auth, messageLog)

    // ---------- Create / Update ----------
    if (exists) {
        callUpdateProduct("${baseURL}/Update", jsonPayload, auth, productId, messageLog)
    } else {
        callCreateProduct("${baseURL}/Create", jsonPayload, auth, productId, messageLog)
    }

    // ---------- Activate ----------
    callActivateProduct("${baseURL}/${URLEncoder.encode(productId,'UTF-8')}/Activate",
                         auth, productId, messageLog)
}

/* Mapping-Funktion: XML → JSON (Product) */
String mapProductToJson(def prodNode) {
    def json = new JsonBuilder()
    json {
        Data {
            ICMProductName          prodNode.Description?.Description?.text()
            ICMProductType          prodNode.ProductTypeCode.text()
            ICMProductCode          prodNode.ProductInternalID.text()
            isActive                (!Boolean.valueOf(prodNode.DeletedIndicator.text())).toString().toLowerCase()
            ICMProductCategoryName  {
                DisplayValue        prodNode.ProductGroupInternalID.text()
            }
        }
    }
    return json.toPrettyString()
}

// =========================================================
// ===============        REST-CALLS          ===============
// =========================================================

/* Prüft, ob Produkt existiert (GET)            */
boolean callProductExists(String url, String productId,
                          String auth, def messageLog) {

    String queryURL = "${url}?${URLEncoder.encode(productId,'UTF-8')}"
    logPayload(messageLog, "Request_CHECK_${productId}", '', 'text/plain')

    HttpURLConnection conn = new URL(queryURL).openConnection() as HttpURLConnection
    conn.setRequestMethod('GET')
    conn.setRequestProperty('Authorization', "Basic ${auth}")

    int code = conn.responseCode
    String rsp = conn.inputStream?.getText('UTF-8') ?: ''
    logPayload(messageLog, "Response_CHECK_${productId}", rsp, 'text/xml')

    if (code == 200 && rsp.toUpperCase().contains('PRODUCT')) {
        return true
    }
    return false
}

/* UPDATE (POST)                                */
void callUpdateProduct(String url, String payload, String auth,
                       String productId, def messageLog) {

    logPayload(messageLog, "Request_UPDATE_${productId}", payload, 'application/json')
    HttpURLConnection conn = (HttpURLConnection)new URL(url).openConnection()
    conn.requestMethod = 'POST'
    conn.doOutput      = true
    conn.setRequestProperty('Authorization', "Basic ${auth}")
    conn.setRequestProperty('Content-Type', 'application/json')
    conn.outputStream.withWriter('UTF-8') { it << payload }

    String rsp = conn.inputStream?.getText('UTF-8') ?: ''
    logPayload(messageLog, "Response_UPDATE_${productId}", rsp, 'application/json')
}

/* CREATE (POST)                                */
void callCreateProduct(String url, String payload, String auth,
                       String productId, def messageLog) {

    logPayload(messageLog, "Request_CREATE_${productId}", payload, 'application/json')
    HttpURLConnection conn = (HttpURLConnection)new URL(url).openConnection()
    conn.requestMethod = 'POST'
    conn.doOutput      = true
    conn.setRequestProperty('Authorization', "Basic ${auth}")
    conn.setRequestProperty('Content-Type', 'application/json')
    conn.outputStream.withWriter('UTF-8') { it << payload }

    String rsp = conn.inputStream?.getText('UTF-8') ?: ''
    logPayload(messageLog, "Response_CREATE_${productId}", rsp, 'application/json')
}

/* ACTIVATE (POST)                              */
void callActivateProduct(String url, String auth,
                         String productId, def messageLog) {

    logPayload(messageLog, "Request_ACTIVATE_${productId}", '', 'text/plain')
    HttpURLConnection conn = (HttpURLConnection)new URL(url).openConnection()
    conn.requestMethod = 'POST'
    conn.setRequestProperty('Authorization', "Basic ${auth}")

    String rsp = conn.inputStream?.getText('UTF-8') ?: ''
    logPayload(messageLog, "Response_ACTIVATE_${productId}", rsp, 'application/json')
}