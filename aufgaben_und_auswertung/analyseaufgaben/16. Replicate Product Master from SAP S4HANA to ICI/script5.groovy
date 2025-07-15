/****************************************************************************************
 *  Groovy-Script:  S4/HANA  ➜  ICI (Buy Side) –  Produkt- & Werk-Stammdaten
 *  Autor       :   Senior CPI Integration Developer
 *  Version     :   1.0
 *  Beschreibung:
 *      – Liest eingehenden XML-Payload
 *      – Validiert Produkte gem. Vorgaben
 *      – Erzeugt JSON-Request-Payloads (Product / Plant)
 *      – Ruft die ICI-REST-Endpunkte (CHECK, CREATE, UPDATE, ACTIVATE)
 *      – Protokolliert sämtlichen Payload als Attachment
 *      – Liefert Fehler sauber über Error-Handling an CPI zurück
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.net.HttpURLConnection
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

/* ------------------------------------------------
 *  Einstiegs­punkt für CPI
 * ------------------------------------------------ */
Message processData(Message message) {

    def messageLog   = messageLogFactory.getMessageLog(message)
    String xmlBody   = message.getBody(String) ?: ''
    logPayload(messageLog, 'IncomingPayload', xmlBody, 'text/xml')

    try {

        /* 1. Header / Property-Vorbereitung */
        Map ctx = prepareContext(message, xmlBody, messageLog)

        /* 2. XML parsen & Produktliste holen */
        def xml      = new XmlSlurper().parseText(xmlBody)
        def products = xml.'**'.findAll { it.name() == 'Product' }

        /* 3. Validierung aller Produkte */
        validateProducts(products, messageLog)

        /* 4. Verarbeitung je Produkt */
        products.each { prodNode ->

            String prodId  = prodNode.ProductInternalID.text()
            ctx.ProductInternalID = prodId          // Produkt-spez. Property setzen

            /* --- PRODUCT Mapping ---------------------------------------------------- */
            String prodJson = mapProduct(prodNode, ctx)
            logPayload(messageLog, "ProductPayload_${prodId}_BeforeCall", prodJson, 'application/json')

            /* --- PRODUCT Existence-Check ------------------------------------------- */
            boolean prodExists = checkProductExists(ctx, prodId, messageLog)

            /* --- PRODUCT Create / Update ------------------------------------------- */
            if (prodExists) {
                updateProduct(ctx, prodJson, messageLog)
            } else {
                createProduct(ctx, prodJson, messageLog)
            }

            /* --- PRODUCT Activate --------------------------------------------------- */
            setActiveStatus(ctx, prodId, messageLog)

            /* --- Werke zum Produkt verarbeiten -------------------------------------- */
            prodNode.Plant.each { plantNode ->

                String plantId = plantNode.PlantID.text()
                ctx.PlantID = plantId               // Werk-spez. Property setzen

                /* Plant-Payload erzeugen */
                String plantJson = mapPlant(plantNode, prodId)
                logPayload(messageLog, "PlantPayload_${prodId}_${plantId}_BeforeCall", plantJson, 'application/json')

                /* Plant Existence-Check */
                boolean plantExists = checkPlantExists(ctx, plantId, messageLog)

                /* Plant Create / Update */
                if (plantExists) {
                    updatePlant(ctx, plantJson, messageLog)
                } else {
                    createPlant(ctx, plantJson, messageLog)
                }
            }
        }

    } catch (Exception ex) {
        handleError(xmlBody, ex, messageLog)
    }

    return message
}

/* =========================================================================
 *  FUNKTION: Kontext (Header / Properties) aufbauen
 * ========================================================================= */
private Map prepareContext(Message msg, String xmlBody, def msgLog) {

    Map<String, String> ctx = [:]

    // 1. Properties aus Message oder Default "placeholder"
    ['requestUser', 'requestPassword', 'requestURL',
     'RecipientBusinessSystemID_config', 'RecipientBusinessSystemID_payload',
     'SenderBusinessSystemID', 'ProductGroupInternalID',
     'ProductInternalID', 'PlantID'].each { key ->
        ctx[key] = (msg.getProperty(key) ?: msg.getHeader(key, String.class)) ?: 'placeholder'
    }

    // 2. Feste Vorgabe laut Aufgabenstellung
    ctx.RecipientBusinessSystemID_config = 'Icertis_BUY'

    // 3. Werte, die erst aus dem XML ersichtlich sind
    def xml = new XmlSlurper().parseText(xmlBody)
    ctx.SenderBusinessSystemID           = xml.'**'.find { it.name() == 'SenderBusinessSystemID' }?.text() ?: ctx.SenderBusinessSystemID
    ctx.RecipientBusinessSystemID_payload = xml.'**'.find { it.name() == 'RecipientBusinessSystemID' }?.text() ?: ctx.RecipientBusinessSystemID_payload

    // 4. Logging
    logPayload(msgLog, 'ResolvedContext', new JsonBuilder(ctx).toPrettyString(), 'application/json')

    return ctx
}

/* =========================================================================
 *  FUNKTION: Produkt­validierung
 * ========================================================================= */
private void validateProducts(def productNodes, def msgLog) {

    def valid = productNodes.findAll { n ->
        def matcher = (n.ProductInternalID.text() =~ /(\d+)/)
        matcher ? matcher[0].toInteger() > 999 : false
    }

    if (valid.isEmpty()) {
        throw new IllegalStateException('Keine Produkte mit Produktnummer > 999 vorhanden – Verarbeitung abgebrochen.')
    }
}

/* =========================================================================
 *  FUNKTION: PRODUCT-Mapping XML ➜ JSON
 * ========================================================================= */
private String mapProduct(def prodNode, Map ctx) {

    /* Transformation ProductTypeCode ➜ DisplayValue */
    String typeCode  = prodNode.ProductTypeCode.text()
    String typeValue = (typeCode?.toUpperCase() == 'FG') ? 'FinishedGood' : typeCode

    /* DeletedIndicator negieren */
    boolean deletedIndicator = prodNode.DeletedIndicator.text()?.toBoolean()
    String isActive          = (!deletedIndicator).toString()

    def json = [
        Data: [
            ICMProductCode            : prodNode.ProductInternalID.text(),
            ICMProductTypeExternalID  : [DisplayValue: typeValue],
            ICMProductGroupCode       : [DisplayValue: prodNode.ProductGroupInternalID.text()],
            ICMExternalSourceSystem   : ctx.SenderBusinessSystemID,
            isActive                  : isActive
        ]
    ]

    return new JsonBuilder(json).toPrettyString()
}

/* =========================================================================
 *  FUNKTION: PLANT-Mapping XML ➜ JSON
 * ========================================================================= */
private String mapPlant(def plantNode, String prodId) {

    def json = [
        Data: [
            ICMPlantID  : [DisplayValue: plantNode.PlantID.text()],
            ICMExternalId: [DisplayValue: prodId]
        ]
    ]

    return new JsonBuilder(json).toPrettyString()
}

/* =========================================================================
 *  FUNKTION: Produkt-Existenz prüfen (GET)
 * ========================================================================= */
private boolean checkProductExists(Map ctx, String prodId, def msgLog) {

    String query    = URLEncoder.encode(prodId, 'UTF-8')
    String endpoint = "${ctx.requestURL}?${query}"
    String response = httpGET(endpoint, ctx, msgLog, "CheckProduct_${prodId}")

    // Produkt existiert, wenn BODY nicht leer und ein Produkt-Objekt enthält
    return response?.toUpperCase().contains('PRODUCT')
}

/* =========================================================================
 *  FUNKTION: Plant-Existenz prüfen (GET)
 * ========================================================================= */
private boolean checkPlantExists(Map ctx, String plantId, def msgLog) {

    String query    = URLEncoder.encode(plantId, 'UTF-8')
    String endpoint = "${ctx.requestURL}?${query}"
    String response = httpGET(endpoint, ctx, msgLog, "CheckPlant_${plantId}")
    return response?.toUpperCase().contains('PLANT')
}

/* =========================================================================
 *  FUNKTION: PRODUCT Create / Update / Activate
 * ========================================================================= */
private void createProduct(Map ctx, String payload, def msgLog) {
    String url = "${ctx.requestURL}/Create"
    httpPOST(url, payload, ctx, msgLog, "CreateProduct_${ctx.ProductInternalID}")
}
private void updateProduct(Map ctx, String payload, def msgLog) {
    String url = "${ctx.requestURL}/Update"
    httpPOST(url, payload, ctx, msgLog, "UpdateProduct_${ctx.ProductInternalID}")
}
private void setActiveStatus(Map ctx, String prodId, def msgLog) {
    String url = "${ctx.requestURL}/${prodId}/Activate"
    httpPOST(url, '', ctx, msgLog, "ActivateProduct_${prodId}")
}

/* =========================================================================
 *  FUNKTION: PLANT Create / Update
 * ========================================================================= */
private void createPlant(Map ctx, String payload, def msgLog) {
    String url = "${ctx.requestURL}/CreatePlant"
    httpPOST(url, payload, ctx, msgLog, "CreatePlant_${ctx.PlantID}")
}
private void updatePlant(Map ctx, String payload, def msgLog) {
    String url = "${ctx.requestURL}/UpdatePlant"
    httpPOST(url, payload, ctx, msgLog, "UpdatePlant_${ctx.PlantID}")
}

/* =========================================================================
 *  FUNKTION: HTTP-GET
 * ========================================================================= */
private String httpGET(String urlStr, Map ctx, def msgLog, String logName) {

    logPayload(msgLog, "${logName}_Request", urlStr, 'text/plain')

    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod('GET')
    setBasicAuth(conn, ctx)

    String response = conn.inputStream?.getText(StandardCharsets.UTF_8.name())
    logPayload(msgLog, "${logName}_Response", response ?: '', 'text/plain')

    return response
}

/* =========================================================================
 *  FUNKTION: HTTP-POST
 * ========================================================================= */
private void httpPOST(String urlStr, String payload, Map ctx, def msgLog, String logName) {

    logPayload(msgLog, "${logName}_RequestBody", payload, 'application/json')

    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod('POST')
    conn.doOutput = true
    conn.setRequestProperty('Content-Type', 'application/json')
    setBasicAuth(conn, ctx)

    if (payload) {
        conn.outputStream.withWriter('UTF-8') { it << payload }
    }

    String resp = conn.inputStream?.getText(StandardCharsets.UTF_8.name())
    logPayload(msgLog, "${logName}_Response", resp ?: '', 'text/plain')
}

/* =========================================================================
 *  FUNKTION: Basic-Auth setzen
 * ========================================================================= */
private void setBasicAuth(HttpURLConnection conn, Map ctx) {
    String auth = "${ctx.requestUser}:${ctx.requestPassword}".getBytes('UTF-8').encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${auth}")
}

/* =========================================================================
 *  FUNKTION: Payload als Attachment in CPI-Log
 * ========================================================================= */
private void logPayload(def msgLog, String name, String content, String mimeType) {
    msgLog?.addAttachmentAsString(name, content ?: '', mimeType)
}

/* =========================================================================
 *  FUNKTION: Zentrales Error-Handling
 * ========================================================================= */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    String errMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errMsg, e)
}