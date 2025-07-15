/*****************************************************************************************
 *  Groovy-Skript zur Integration von S/4 HANA in ICI (Buy Side)                          *
 *                                                                                        *
 *  Die Implementierung erfüllt folgende Anforderungen:                                   *
 *  • Modularer Aufbau (separate Funktionen)                                              *
 *  • Umfangreiches Error-Handling inkl. Payload-Attachment                               *
 *  • Logging an allen relevanten Stellen                                                 *
 *  • Validierung, Mapping (XML → JSON) & API-Aufrufe                                     *
 *                                                                                        *
 *  Autor:  SAP CPI – Senior Integration Developer                                        *
 *****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.net.HttpURLConnection
import java.net.URLEncoder

/* ===================================================================================== *
 *  Haupt-Einstiegspunkt – wird von SAP CPI automatisch aufgerufen                        *
 * ===================================================================================== */
Message processData(Message message) {
    def messageLog   = messageLogFactory.getMessageLog(message)
    def originalBody = message.getBody(String) ?: ''

    try {
        /* ----- Original-Payload sichern ------------------------------------------------ */
        logMessage(messageLog, 'IncomingPayload', originalBody)

        /* ----- Header & Properties ermitteln ------------------------------------------- */
        def props = setPropertiesAndHeaders(message)

        /* ----- XML parsen & Produkte ermitteln ----------------------------------------- */
        def xml       = new XmlSlurper()
                            .parseText(originalBody)
                            .declareNamespace(ns: 'http://sap.com/xi/APPL/Global2')
        def products  = xml.'**'.findAll { it.name() == 'Product' }

        if (!products) {
            throw new IllegalStateException('Keine <Product>-Tags im eingehenden Payload gefunden.')
        }

        /* ----- Validierung & Verarbeitung pro Produkt ---------------------------------- */
        def validProducts = products.findAll { validateProduct(it) }
        if (!validProducts) {
            throw new IllegalStateException('Kein Produkt mit Produktnummer > 999 vorhanden.')
        }

        validProducts.each { product ->
            /* ----- Produkt-Spezifische Properties -------------------------------------- */
            props.ProductInternalID       = product.ProductInternalID.text()
            props.ProductGroupInternalID  = product.ProductGroupInternalID.text() ?: ''

            /* ----- Mapping Produkt ------------------------------------------------------ */
            def productJson = mapProductToJson(product, props)
            logMessage(messageLog, "MappedProduct_${props.ProductInternalID}", productJson)

            /* ----- API-Call: Existenzprüfung ------------------------------------------- */
            def exists = callApiCheckProductExists(props, messageLog)

            /* ----- API-Call: CREATE / UPDATE ------------------------------------------- */
            if (exists) {
                callApiUpdateProduct(props, productJson, messageLog)
            } else {
                callApiCreateProduct(props, productJson, messageLog)
            }

            /* ----- API-Call: Aktiv setzen ---------------------------------------------- */
            callApiSetActiveStatus(props, messageLog)

            /* ----- Werke verarbeiten ---------------------------------------------------- */
            product.Plant.each { plant ->
                props.PlantID = plant.PlantID.text()

                def plantJson = mapPlantToJson(plant, props)
                logMessage(messageLog, "MappedPlant_${props.PlantID}", plantJson)

                def plantExists = callApiCheckPlantExists(props, messageLog)

                if (plantExists) {
                    callApiUpdatePlant(props, plantJson, messageLog)
                } else {
                    callApiCreatePlant(props, plantJson, messageLog)
                }
            }
        }

        return message                                                                   // Verarbeitung erfolgreich
    } catch (Exception e) {
        handleError(originalBody, e, messageLog)                                         // zentrales Error-Handling
    }
    return message                                                                       // (unerreichbar)
}

/* ===================================================================================== *
 *  Hilfs- / Utility-Funktionen                                                          *
 * ===================================================================================== */

/* -------- Logging -------------------------------------------------------------------- */
def logMessage(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content ?: '', 'text/plain')
}

/* -------- Header & Properties -------------------------------------------------------- */
Map setPropertiesAndHeaders(Message message) {
    Map props = [:]

    /* Bereits gesetzte Properties übernehmen oder Platzhalter vergeben */
    props.requestUser   = message.getProperty('requestUser')   ?: 'placeholder'
    props.requestPassword = message.getProperty('requestPassword') ?: 'placeholder'
    props.requestURL    = message.getProperty('requestURL')    ?: 'placeholder'

    /* Business-System */
    props.SenderBusinessSystemID = (message.getHeaders()['SenderBusinessSystemID']
                                  ?: message.getProperty('SenderBusinessSystemID')
                                  ?: 'placeholder')
    props.RecipientBusinessSystemID_config = 'Icertis_BUY'

    return props
}

/* -------- Validierung ---------------------------------------------------------------- */
boolean validateProduct(def productNode) {
    def numPart = productNode.ProductInternalID.text().replaceAll(/\D/, '')             // nur Ziffern
    return numPart && numPart.toInteger() > 999
}

/* -------- Mapping: Produkt ----------------------------------------------------------- */
String mapProductToJson(def productNode, Map props) {
    boolean deleted = productNode.DeletedIndicator.text()?.equalsIgnoreCase('true')
    def json = new JsonBuilder()
    json {
        Data {
            ICMProductCode             productNode.ProductInternalID.text()
            ICMProductTypeExternalID {
                DisplayValue           mapProductTypeDisplay(productNode.ProductTypeCode.text())
            }
            ICMProductGroupCode {
                DisplayValue           productNode.ProductGroupInternalID.text()
            }
            ICMExternalSourceSystem    props.SenderBusinessSystemID
            isActive                  (!deleted).toString()                              // Negation!
        }
    }
    return json.toPrettyString()
}

/* -------- Mapping: Werk ------------------------------------------------------------- */
String mapPlantToJson(def plantNode, Map props) {
    def json = new JsonBuilder()
    json {
        Data {
            ICMPlantID {
                DisplayValue           props.PlantID
            }
            ICMExternalId {
                DisplayValue           props.ProductInternalID
            }
        }
    }
    return json.toPrettyString()
}

/* -------- Mapping-Helfer: Produkt-Typ → DisplayValue -------------------------------- */
String mapProductTypeDisplay(String typeCode) {
    switch (typeCode?.toUpperCase()) {
        case 'FG': return 'FinishedGood'
        default  : return typeCode ?: ''
    }
}

/* ===================================================================================== *
 *  API-Aufrufe                                                                          *
 * ===================================================================================== */
boolean callApiCheckProductExists(Map p, def log) {
    def url = "${p.requestURL}?id=${URLEncoder.encode(p.ProductInternalID, 'UTF-8')}"
    def r   = executeHttp('GET', url, p, null, log, "CheckProduct_${p.ProductInternalID}")
    return r.body?.toUpperCase()?.contains('PRODUCT')
}

void callApiUpdateProduct(Map p, String body, def log) {
    executeHttp('POST', "${p.requestURL}/Update", p, body, log, "UpdateProduct_${p.ProductInternalID}")
}

void callApiCreateProduct(Map p, String body, def log) {
    executeHttp('POST', "${p.requestURL}/Create", p, body, log, "CreateProduct_${p.ProductInternalID}")
}

void callApiSetActiveStatus(Map p, def log) {
    def url = "${p.requestURL}/${URLEncoder.encode(p.ProductInternalID, 'UTF-8')}/Activate"
    executeHttp('POST', url, p, '', log, "ActivateProduct_${p.ProductInternalID}")
}

boolean callApiCheckPlantExists(Map p, def log) {
    def url = "${p.requestURL}?id=${URLEncoder.encode(p.PlantID, 'UTF-8')}"
    def r   = executeHttp('GET', url, p, null, log, "CheckPlant_${p.PlantID}")
    return r.body?.toUpperCase()?.contains('PLANT')
}

void callApiUpdatePlant(Map p, String body, def log) {
    executeHttp('POST', "${p.requestURL}/UpdatePlant", p, body, log, "UpdatePlant_${p.PlantID}")
}

void callApiCreatePlant(Map p, String body, def log) {
    executeHttp('POST', "${p.requestURL}/CreatePlant", p, body, log, "CreatePlant_${p.PlantID}")
}

/* -------- HTTP-Helper --------------------------------------------------------------- */
Map executeHttp(String method, String url, Map p, String body,
                def log, String prefix) {
    logMessage(log, "${prefix}_RequestURL", url)
    if (body != null) logMessage(log, "${prefix}_RequestBody", body)

    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    conn.setRequestMethod(method)
    conn.setRequestProperty('Authorization',
        'Basic ' + "${p.requestUser}:${p.requestPassword}".bytes.encodeBase64().toString())
    conn.setRequestProperty('Accept', 'application/json')

    if (method == 'POST') {
        conn.doOutput = true
        conn.setRequestProperty('Content-Type', 'application/json')
        conn.outputStream.withWriter('UTF-8') { it << (body ?: '') }
    }

    def code     = conn.responseCode
    def respBody = ''
    try { respBody = conn.inputStream?.getText('UTF-8') } 
    catch (Exception ignore) { respBody = conn.errorStream?.getText('UTF-8') }

    logMessage(log, "${prefix}_ResponseCode", code.toString())
    logMessage(log, "${prefix}_ResponseBody", respBody)

    return [code: code, body: respBody]
}

/* ===================================================================================== *
 *  Zentrales Error-Handling                                                             *
 * ===================================================================================== */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    throw new RuntimeException("Fehler im Groovy-Skript: ${e.message}", e)
}