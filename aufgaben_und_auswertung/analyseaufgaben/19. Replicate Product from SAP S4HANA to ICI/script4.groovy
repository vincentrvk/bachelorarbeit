/*************************************************************************
 *  Groovy-Skript – Produkt-Integration S/4HANA -> ICI (Sell-Side)
 *
 *  Autor:   Senior Integration Developer
 *  Version: 1.0
 *
 *  Dieses Skript erfüllt folgende Anforderungen:
 *    • Aufbereitung eingehender Produkt-Stammdaten (XML)
 *    • Prüfung & Aufteilung der Produkte
 *    • Dynamische Entscheidung CREATE / UPDATE
 *    • Aktivierung des Produktes
 *    • Vollständiges Logging & Fehlermanagement
 *************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.net.HttpURLConnection
import java.nio.charset.StandardCharsets
import java.util.regex.Matcher
import java.util.regex.Pattern

/* ================================================================
 *                H A U P T P R O Z E S S
 * ================================================================ */
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    try {

        /* 1) Eingehenden Payload sichern */
        final String inboundXml = message.getBody(String) ?: ""
        logPayload(messageLog, 'InboundPayload', inboundXml)

        /* 2) Header & Properties setzen/auffüllen                            */
        setContextData(message, inboundXml, messageLog)

        /* 3) Produkte splitten & validieren                                 */
        def products = splitProducts(inboundXml)
        def validProducts = products.findAll { validateProduct(it) }

        if (validProducts.isEmpty()) {
            throw new IllegalStateException(
                    'Keine Produkte mit Produktnummer > 999 gefunden – Verarbeitung abgebrochen.')
        }

        /* 4) Jedes valide Produkt einzeln verarbeiten                       */
        validProducts.each { prodNode ->

            final String productId = prodNode.ProductInternalID.text()

            /* 4.1) Existenzprüfung ---------------------------------------- */
            boolean exists = checkProductExists(productId, message, messageLog)

            /* 4.2) Request-Mapping --------------------------------------- */
            logPayload(messageLog, 'BeforeMapping_' + productId, groovy.xml.XmlUtil.serialize(prodNode))
            final String jsonBody = mapProduct(prodNode)
            logPayload(messageLog, 'AfterMapping_' + productId, jsonBody)

            /* 4.3) Produkt anlegen / aktualisieren ------------------------ */
            if (exists) {
                callUpdateProduct(jsonBody, message, messageLog)
            } else {
                callCreateProduct(jsonBody, message, messageLog)
            }

            /* 4.4) Aktivieren --------------------------------------------- */
            callActivateProduct(productId, message, messageLog)
        }

        return message

    } catch (Exception e) {
        handleError(inboundXml, e, messageLog)
        return message          // wird durch handleError nie erreicht
    }
}

/* ================================================================
 *      F U N K T I O N E N   –   C O N T E X T / U T I L
 * ================================================================ */

/**
 * Setzt fehlende Properties & Header (Placeholder, falls nicht vorhanden)
 * und liest relevante Daten aus dem XML.
 */
private void setContextData(Message msg, String xmlBody, def logger) {

    def slurper = new XmlSlurper().parseText(xmlBody)
    String senderBS   = slurper?.MessageHeader?.SenderBusinessSystemID?.text()      ?: 'placeholder'
    String recipientBS= slurper?.MessageHeader?.RecipientBusinessSystemID?.text()   ?: 'placeholder'

    // Header / Properties mit Default-Platzhaltern
    Map contextDefaults = [
            requestUser                   : 'placeholder',
            requestPassword               : 'placeholder',
            requestURL                    : 'https://placeholder',
            RecipientBusinessSystemID_config : 'Icertis_SELL',
            RecipientBusinessSystemID_payload: recipientBS,
            SenderBusinessSystemID           : senderBS
    ]

    contextDefaults.each { k, v ->
        if (msg.getProperty(k) == null) { msg.setProperty(k, v) }
    }

    logPayload(logger, 'ContextPropertiesAfterInit', contextDefaults.toString())
}

/**
 * Zerlegt den XML-Payload in einzelne <Product>-Nodes.
 */
private List splitProducts(String xml) {
    def root = new XmlSlurper().parseText(xml)
    return root?.ProductMDMReplicateRequestMessage?.Product?.collect { it } ?: []
}

/**
 * Validiert ein Produkt (Produktnummer > 999).
 */
private boolean validateProduct(prodNode) {

    String idVal = prodNode?.ProductInternalID?.text() ?: ''
    Matcher m = (idVal =~ /(\d+)/)          // erste Zahl im Produkt-Key
    if (!m.find()) { return false }

    int num = Integer.parseInt(m.group(1))
    return num > 999
}

/**
 * Erstellt den JSON-Body gemäss Mapping-Spezifikation.
 */
private String mapProduct(prodNode) {

    String desc        = prodNode?.Description?.Description?.text() ?: ''
    String prodType    = prodNode?.ProductTypeCode?.text()          ?: ''
    String prodCode    = prodNode?.ProductInternalID?.text()        ?: ''
    String category    = prodNode?.ProductGroupInternalID?.text()   ?: ''
    String deletedInd  = prodNode?.DeletedIndicator?.text()         ?: 'false'

    boolean deletedBool = deletedInd.equalsIgnoreCase('true')
    String isActive     = (!deletedBool).toString()                 // Negation

    def json = new JsonBuilder()
    json {
        Data {
            ICMProductName          desc
            ICMProductType          prodType
            ICMProductCode          prodCode
            isActive                isActive
            ICMProductCategoryName  {
                DisplayValue        category
            }
        }
    }
    return json.toPrettyString()
}

/**
 * Prüft per GET, ob Produkt bereits existiert.
 * Rückgabe: true = existiert, false = muss erstellt werden.
 */
private boolean checkProductExists(String productId, Message msg, def logger) {

    final String url       = "${msg.getProperty('requestURL')}/${URLEncoder.encode(productId, 'UTF-8')}"
    final String authToken = createAuth(msg)

    logPayload(logger, 'CheckExistence_Request_' + productId, url)

    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    conn.setRequestMethod('GET')
    conn.setRequestProperty('Authorization', "Basic ${authToken}")
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(15000)

    final int rc = conn.responseCode
    String respBody = conn.inputStream?.getText('UTF-8') ?: ''
    logPayload(logger, 'CheckExistence_Response_' + productId, "RC=$rc\n$respBody")

    return respBody?.toUpperCase()?.contains('PRODUCT')      // lt. Anforderung
}

/**
 * Sendet UPDATE-Request.
 */
private void callUpdateProduct(String body, Message msg, def logger) {
    final String url = "${msg.getProperty('requestURL')}/Update"
    callPost(url, body, msg, logger, 'UPDATE')
}

/**
 * Sendet CREATE-Request.
 */
private void callCreateProduct(String body, Message msg, def logger) {
    final String url = "${msg.getProperty('requestURL')}/Create"
    callPost(url, body, msg, logger, 'CREATE')
}

/**
 * Sendet Activate-Request.
 */
private void callActivateProduct(String productId, Message msg, def logger) {
    final String url = "${msg.getProperty('requestURL')}/${URLEncoder.encode(productId,'UTF-8')}/Activate"
    callPost(url, '', msg, logger, 'ACTIVATE')
}

/**
 * Generische POST-Methode inkl. Logging.
 */
private void callPost(String url, String body, Message msg, def logger, String step) {

    logPayload(logger, "${step}_Request", "URL: $url\n\n$body")

    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Authorization', "Basic ${createAuth(msg)}")
    conn.setRequestProperty('Content-Type', 'application/json')
    conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { it << body }

    int rc = conn.responseCode
    String resp = ''
    if (rc >= 200 && rc < 300) {
        resp = conn.inputStream?.getText('UTF-8') ?: ''
    } else {
        resp = conn.errorStream?.getText('UTF-8') ?: ''
    }
    logPayload(logger, "${step}_Response", "RC=$rc\n$resp")
}

/**
 * Erstellt den Base64-Auth-Token anhand Properties.
 */
private String createAuth(Message msg) {
    String user = msg.getProperty('requestUser')     ?: ''
    String pwd  = msg.getProperty('requestPassword') ?: ''
    return "${user}:${pwd}".bytes.encodeBase64().toString()
}

/**
 * Fügt den Payload als Attachment dem Trace hinzu.
 */
private void logPayload(def logger, String name, String payload) {
    logger?.addAttachmentAsString(name, payload ?: '', 'text/plain')
}

/* ================================================================
 *      F U N K T I O N E N   –   E R R O R   H A N D L I N G
 * ================================================================ */

/**
 * Globale Fehlerbehandlung – wirft RuntimeException mit Klartext.
 */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    String errorMsg = "Fehler im Integrationsskript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}