/********************************************************************************************
 *  S4/HANA → ICI  Produkt-Master  (Sell-Side)                                                *
 *  ---------------------------------------------------------------------------------------- *
 *  Dieses Skript übernimmt:                                                                 *
 *  • Auslesen & Validieren der eingehenden XML-Payload                                       *
 *  • Ermittlung aller benötigten Header/Properties                                          *
 *  • Aufruf der ICI-REST-Schnittstellen (Check, Create/Update, Activate)                    *
 *  • Mapping XML → JSON                                                                     *
 *  • Umfangreiches Logging & zentrales Error-Handling                                       *
 *                                                                                           *
 *  Autor:  Senior-Integration-Developer                                                     *
 ********************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder

Message processData(Message message) {

    /*----------------------------------------------------------*
     | Initiales Setup                                           |
     *----------------------------------------------------------*/
    def messageLog = messageLogFactory.getMessageLog(message)
    def rawInput   = message.getBody(String)                    // Original-Payload sichern
    logPayload("RAW_InputPayload", rawInput, messageLog)

    try {

        /*------------------------------------------------------*
         | Header & Properties ermitteln/setzen                  |
         *------------------------------------------------------*/
        setDefaultValues(message, rawInput)

        /*------------------------------------------------------*
         | Produkte ermitteln & verarbeiten                      |
         *------------------------------------------------------*/
        def xml     = new XmlSlurper().parseText(rawInput)
        def productNodes = xml.'**'.findAll { it.localName() == 'Product' }

        // Validierung: mindestens ein Produkt mit Nummer > 999
        def validNodes = productNodes.findAll { validateProduct(it) }
        if (!validNodes) {
            throw new IllegalStateException("Kein gültiges Produkt (Nummer > 999) im Payload gefunden.")
        }

        // Ergebnisse sammeln
        def results = []

        validNodes.each { prod ->
            // Produktspezifische Properties setzen
            message.setProperty('ProductInternalID', prod.ProductInternalID.text())
            message.setProperty('ProductGroupInternalID', prod.ProductGroupInternalID.text())

            /*----------------------------------------------*
             | Schritt 1: Prüfen, ob Produkt existiert       |
             *----------------------------------------------*/
            def existsResp = callCheckExists(message, messageLog)
            def exists = existsResp.exists
            results << [productCode: prod.ProductInternalID.text(), exists: exists]

            /*----------------------------------------------*
             | Schritt 2: Mapping vorbereiten                |
             *----------------------------------------------*/
            def jsonPayload = mappingProduct(prod)
            logPayload("Mapped_Payload_${prod.ProductInternalID.text()}", jsonPayload, messageLog)

            /*----------------------------------------------*
             | Schritt 3: Create -oder- Update               |
             *----------------------------------------------*/
            if (exists) {
                callUpdate(jsonPayload, message, messageLog)
            } else {
                callCreate(jsonPayload, message, messageLog)
            }

            /*----------------------------------------------*
             | Schritt 4: Aktivieren                         |
             *----------------------------------------------*/
            callActivate(message, messageLog)
        }

        // Sammelergebnis zurückgeben (nur informativ)
        message.setBody(new JsonBuilder([result: results]).toPrettyString())
        return message

    } catch (Exception e) {
        // zentrales Error-Handling
        handleError(rawInput, e, messageLog)
        return message   // wird nie erreicht – handleError wirft Exception
    }
}

/* =================================================================================================
 *  Hilfs-Funktionen
 * ================================================================================================= */

/**
 * Setzt alle benötigten Header/Properties.
 * Fehlt ein Wert, wird „placeholder“ verwendet.
 */
def setDefaultValues(Message message, String xmlString) {
    def xml = new XmlSlurper().parseText(xmlString)

    // Werte aus Payload
    def senderBS     = xml.'**'.find { it.localName() == 'SenderBusinessSystemID' }?.text()      ?: 'placeholder'
    def recipientBS  = xml.'**'.find { it.localName() == 'RecipientBusinessSystemID' }?.text()   ?: 'placeholder'

    // Defaults / vorhandene Properties übernehmen
    ['requestUser', 'requestPassword', 'requestURL'].each { key ->
        if (!message.getProperty(key)) {
            message.setProperty(key, 'placeholder')
        }
    }

    message.setProperty('RecipientBusinessSystemID_config', 'Icertis_SELL')
    message.setProperty('RecipientBusinessSystemID_payload', recipientBS)
    message.setProperty('SenderBusinessSystemID',            senderBS)
}

/**
 * Produkt-Validierung gem. Regel:
 *   „Es werden nur Produkte mit Produktnummer höher als 999 verarbeitet“
 */
boolean validateProduct(prodNode) {
    def prodId = prodNode.ProductInternalID.text()
    def numPart = prodId.replaceAll(/[^0-9]/, '')               // nur Ziffern herauslösen
    return (numPart.isInteger() && numPart.toInteger() > 999)
}

/**
 * Erstellt das JSON-Mapping nach Vorgabe.
 */
String mappingProduct(prodNode) {
    def deleted     = prodNode.DeletedIndicator.text()
    def isActiveVal = !(deleted?.equalsIgnoreCase('true'))      // Negation

    def json = new JsonBuilder()
    json {
        Data {
            ICMProductName         prodNode.Description.Description.text()
            ICMProductType         prodNode.ProductTypeCode.text()
            ICMProductCode         prodNode.ProductInternalID.text()
            isActive               "${isActiveVal}"
            ICMProductCategoryName {
                DisplayValue       prodNode.ProductGroupInternalID.text()
            }
        }
    }
    return json.toPrettyString()
}

/**
 * Prüft, ob das Produkt bereits in ICI existiert (GET).
 * Rückgabe: [exists: true/false, response: String]
 */
def callCheckExists(Message message, def messageLog) {
    def baseUrl  = message.getProperty('requestURL')
    def user     = message.getProperty('requestUser')
    def pwd      = message.getProperty('requestPassword')
    def prodCode = message.getProperty('ProductInternalID')

    def url      = new URL("${baseUrl}?${URLEncoder.encode(prodCode,'UTF-8')}")
    logPayload("REQ_CheckExists_${prodCode}", url.toString(), messageLog)

    HttpURLConnection con = (HttpURLConnection) url.openConnection()
    con.setRequestMethod('GET')
    con.setRequestProperty('Authorization', "Basic ${(user+':'+pwd).bytes.encodeBase64().toString()}")
    con.connect()

    def respBody = con.inputStream.text
    logPayload("RESP_CheckExists_${prodCode}", respBody, messageLog)

    boolean exists = respBody.toUpperCase().contains('PRODUCT')
    return [exists: exists, response: respBody]
}

/**
 * Ruft das UPDATE-Endpoint (POST) auf.
 */
def callUpdate(String jsonPayload, Message message, def messageLog) {
    def baseUrl  = message.getProperty('requestURL') + "/Update"
    makePostCall(baseUrl, jsonPayload, 'UPDATE', message, messageLog)
}

/**
 * Ruft das CREATE-Endpoint (POST) auf.
 */
def callCreate(String jsonPayload, Message message, def messageLog) {
    def baseUrl  = message.getProperty('requestURL') + "/Create"
    makePostCall(baseUrl, jsonPayload, 'CREATE', message, messageLog)
}

/**
 * Ruft das Activate-Endpoint (POST) auf.
 */
def callActivate(Message message, def messageLog) {
    def prodCode = message.getProperty('ProductInternalID')
    def url      = "${message.getProperty('requestURL')}/${URLEncoder.encode(prodCode,'UTF-8')}/Activate"
    makePostCall(url, '', 'ACTIVATE', message, messageLog)
}

/**
 * Generische POST-Methode (JSON oder leer).
 */
def makePostCall(String urlStr, String payload, String tag, Message message, def messageLog) {
    logPayload("REQ_${tag}_${message.getProperty('ProductInternalID')}", payload ?: '<<empty body>>', messageLog)

    def user = message.getProperty('requestUser')
    def pwd  = message.getProperty('requestPassword')
    HttpURLConnection con = (HttpURLConnection) new URL(urlStr).openConnection()
    con.setRequestMethod('POST')
    con.setRequestProperty('Content-Type', 'application/json;charset=UTF-8')
    con.setRequestProperty('Authorization', "Basic ${(user+':'+pwd).bytes.encodeBase64().toString()}")
    con.doOutput = true
    if (payload) {
        con.outputStream.withWriter("UTF-8") { it << payload }
    }

    def resp = con.inputStream.text
    logPayload("RESP_${tag}_${message.getProperty('ProductInternalID')}", resp, messageLog)
}

/**
 * Fügt Payload als Attachment im Monitoring hinzu.
 */
def logPayload(String name, String payload, def messageLog) {
    messageLog?.addAttachmentAsString(name, payload ?: "<<null>>", "text/plain")
}

/**
 * Zentrales Error-Handling – wirft RuntimeException,
 * Payload wird als Attachment mitgegeben.
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}