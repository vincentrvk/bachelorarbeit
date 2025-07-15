/*************************************************************************
 *  SAP Cloud Integration – Groovy Script                                 *
 *  Aufgabe: Produkt- & Werk-Synchronisation S/4 ↔ ICI (Buy-Side)         *
 *                                                                        *
 *  Autor:  (generated)                                                   *
 *************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.Base64

/* ======================================================================
   =  Main Entry Point                                                   =
   ====================================================================== */
Message processData(Message message) {
    // Original Body sichern (wird im Error-Fall angehängt)
    final String originalBody = message.getBody(String) ?: ''
    final def       msgLog     = messageLogFactory.getMessageLog(message)

    try {
        /* 1) Header & Property-Initialisierung */
        initContext(message, msgLog)

        /* 2) XML parsen  */
        final def xml        = new XmlSlurper().parseText(originalBody)
        final def productLst = xml.'**'.findAll { it.name() == 'Product' }

        /* 3) Validierung – Mindestens ein gültiges Produkt */
        final def validProducts = productLst.findAll { isValidProduct(it) }
        if (!validProducts) {
            handleError(originalBody,
                        new Exception('Kein Produkt mit Nummer > 999 im Payload gefunden.'),
                        msgLog)
        }

        /* 4) Verarbeitung je Produkt */
        validProducts.each { prodNode ->
            final String prodId = prodNode.ProductInternalID.text()

            message.setProperty('ProductInternalID',     prodId)
            message.setProperty('ProductGroupInternalID', prodNode.ProductGroupInternalID.text())

            /* 4.1) Existenzcheck Produkt */
            final boolean prodExists = checkExistence(message, prodId, msgLog, 'PRODUCT')

            /* 4.2) Mapping → JSON */
            addLogAttachment(msgLog, "ProductInput-${prodId}.xml", prodNode.toString())
            final String prodJson = mapProductJson(prodNode, message)
            addLogAttachment(msgLog, "ProductPayload-${prodId}.json", prodJson)

            /* 4.3) Anlegen / Aktualisieren */
            if (prodExists) {
                callProductService(message, '/Update', 'POST', prodJson,
                                   "UPDATE Product ${prodId}", msgLog)
            } else {
                callProductService(message, '/Create', 'POST', prodJson,
                                   "CREATE Product ${prodId}", msgLog)
            }

            /* 4.4) Aktivieren */
            callProductService(message, "/${prodId}/Activate", 'POST', prodJson,
                               "ACTIVATE Product ${prodId}", msgLog)

            /* 4.5) Werke verarbeiten */
            prodNode.Plant.each { plantNode ->
                final String plantId = plantNode.PlantID.text()
                message.setProperty('PlantID', plantId)

                final boolean plantExists = checkExistence(message, plantId, msgLog, 'PLANT')

                addLogAttachment(msgLog, "PlantInput-${plantId}.xml", plantNode.toString())
                final String plantJson = mapPlantJson(plantNode, prodId)
                addLogAttachment(msgLog, "PlantPayload-${plantId}.json", plantJson)

                if (plantExists) {
                    callProductService(message, '/UpdatePlant', 'POST', plantJson,
                                       "UPDATE Plant ${plantId}", msgLog)
                } else {
                    callProductService(message, '/CreatePlant', 'POST', plantJson,
                                       "CREATE Plant ${plantId}", msgLog)
                }
            }
        }

        /* Body wiederherstellen (wird weitergeleitet) */
        message.setBody(originalBody)
        return message
    } catch (Exception e) {
        handleError(originalBody, e, msgLog)   // wirft RuntimeException
        return message                         // nie erreicht; Compiler-Beruhigung
    }
}

/* ======================================================================
   =  Hilfs- & Service-Funktionen                                        =
   ====================================================================== */

/*  initContext:
 *  Befüllt Properties & Header mit vorhandenen Werten oder „placeholder“.
 */
def initContext(Message message, def msgLog) {
    /* Properties */
    ['requestUser',
     'requestPassword',
     'requestURL',
     'RecipientBusinessSystemID_config',
     'RecipientBusinessSystemID_payload',
     'SenderBusinessSystemID'].each { key ->
        if (message.getProperty(key) == null) {
            message.setProperty(key, (key == 'RecipientBusinessSystemID_config')
                                        ? 'Icertis_BUY'
                                        : 'placeholder')
        }
    }

    /* Header (nur Platzhalter setzen, falls nicht vorhanden) */
    ['requestUser', 'requestPassword', 'requestURL'].each { hdr ->
        if (message.getHeader(hdr, String) == null) {
            message.setHeader(hdr, message.getProperty(hdr))
        }
    }

    msgLog?.addAttachmentAsString('ContextAfterInit',
                                  new groovy.json.JsonBuilder(message.getProperties()).toPrettyString(),
                                  'application/json')
}

/*  isValidProduct:
 *  Prüft, ob Produktnummer numerisch > 999 ist.
 */
boolean isValidProduct(def productNode) {
    final String idTxt = productNode.ProductInternalID.text()
    final String numeric = idTxt.replaceAll('[^0-9]', '')
    return numeric.isInteger() && numeric.toInteger() > 999
}

/*  mapProductJson:
 *  Erzeugt JSON-Payload gemäss Mappingspezifikation.
 */
String mapProductJson(def productNode, Message message) {
    final String prodId  = productNode.ProductInternalID.text()
    final String typeCd  = productNode.ProductTypeCode.text()
    final String grpCode = productNode.ProductGroupInternalID.text()
    final String srcSys  = message.getProperty('SenderBusinessSystemID') ?: 'placeholder'
    final String active  = !(productNode.DeletedIndicator.text()?.equalsIgnoreCase('true'))).toString()

    def builder = new JsonBuilder()
    builder {
        Data {
            ICMProductCode          prodId
            ICMProductTypeExternalID {
                DisplayValue resolveProductType(typeCd)
            }
            ICMProductGroupCode {
                DisplayValue grpCode
            }
            ICMExternalSourceSystem srcSys
            isActive                active
        }
    }
    return builder.toPrettyString()
}

/*  mapPlantJson:
 *  Erzeugt JSON-Payload für Werk gemäss Mappingspezifikation.
 */
String mapPlantJson(def plantNode, String productId) {
    final String plantId = plantNode.PlantID.text()
    def builder = new JsonBuilder()
    builder {
        Data {
            ICMPlantID {
                DisplayValue plantId
            }
            ICMExternalId {
                DisplayValue productId
            }
        }
    }
    return builder.toPrettyString()
}

/*  resolveProductType:
 *  Beispielhafte Umwandlung des ProductTypeCode → DisplayValue.
 *  Bei Bedarf Mapping-Tabelle ergänzen.
 */
String resolveProductType(String typeCode) {
    switch (typeCode?.toUpperCase()) {
        case 'FG':   return 'FinishedGood'
        default:     return typeCode
    }
}

/*  checkExistence:
 *  Führt GET-Aufruf aus um Existenz (Produkt | Werk) zu prüfen.
 *  Rückgabe: true = existiert.
 */
boolean checkExistence(Message message, String key, def msgLog, String entityType) {
    final String urlBase = message.getProperty('requestURL')
    final String user    = message.getProperty('requestUser')
    final String pwd     = message.getProperty('requestPassword')

    final String fullUrl = "${urlBase}?${key}"
    addLogAttachment(msgLog, "Check-${entityType}-${key}-REQ.txt", fullUrl)

    final HttpURLConnection conn = (HttpURLConnection) new URL(fullUrl).openConnection()
    conn.requestMethod           = 'GET'
    conn.setRequestProperty('Authorization', basicAuth(user, pwd))

    final int rc   = conn.responseCode
    final String r = conn.inputStream?.getText('UTF-8') ?: ''
    addLogAttachment(msgLog, "Check-${entityType}-${key}-RESP.txt", r)

    return rc == 200 && r?.contains(entityType)
}

/*  callProductService:
 *  Führt POST-Aufruf für Create/Update/Aktivieren durch.
 */
void callProductService(Message message,
                        String path,
                        String method,
                        String body,
                        String logTag,
                        def   msgLog) {

    final String urlBase = message.getProperty('requestURL')
    final String user    = message.getProperty('requestUser')
    final String pwd     = message.getProperty('requestPassword')
    final String fullUrl = urlBase + path

    addLogAttachment(msgLog, "${logTag}-REQ.json", body)

    HttpURLConnection conn = (HttpURLConnection) new URL(fullUrl).openConnection()
    conn.requestMethod = method
    conn.doOutput      = true
    conn.setRequestProperty('Authorization', basicAuth(user, pwd))
    conn.setRequestProperty('Content-Type', 'application/json; charset=UTF-8')

    conn.outputStream.withWriter('UTF-8') { it << body }
    final int rc = conn.responseCode
    final String resp = rc >= 200 && rc < 300 ? conn.inputStream?.getText('UTF-8') : conn.errorStream?.getText('UTF-8')

    addLogAttachment(msgLog, "${logTag}-RESP.txt", "HTTP ${rc}\n${resp ?: ''}")
}

/*  basicAuth:
 *  Liefert Header-String für Basic-Authentication.
 */
String basicAuth(String user, String pwd) {
    final String creds = "${user}:${pwd}"
    final String enc   = Base64.encoder.encodeToString(creds.getBytes(StandardCharsets.UTF_8))
    return "Basic ${enc}"
}

/*  addLogAttachment:
 *  Fügt Attachment an MessageLog an (falls Log-Level aktiviert).
 */
void addLogAttachment(def msgLog, String name, String content) {
    msgLog?.addAttachmentAsString(name, content, 'text/plain')
}

/*  handleError:
 *  Zentrales Error-Handling inkl. Attachment des Ursprungs-Payloads.
 */
def handleError(String body, Exception e, def msgLog) {
    msgLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    final String err = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(err, e)
}