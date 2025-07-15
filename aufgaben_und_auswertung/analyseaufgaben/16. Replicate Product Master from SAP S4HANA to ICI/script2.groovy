/****************************************************************************************
 *  Author  : Senior-Developer – SAP Cloud Integration
 *  Version : 1.0
 *  Purpose : Zentrales Groovy-Skript für die Buy-Side-Produkt­integration nach ICI
 *            – setzt Properties/Header
 *            – führt Validierung durch
 *            – erzeugt JSON-Request-Payloads (Product / Plant)
 *            – stellt umfangreiches Logging & Error-Handling bereit
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.nio.charset.StandardCharsets

/* =========================================================================
 *  Einstiegspunkt
 * ------------------------------------------------------------------------- */
Message processData(Message message) {

    /* Original-Payload sichern                                               */
    final String originalBody = message.getBody(String) ?: ''
    final def log         = messageLogFactory.getMessageLog(message)
    savePayloadAsAttachment(log, 'IncomingPayload', originalBody)

    try {
        /* Properties & Header ermitteln / setzen                             */
        setHeadersAndProperties(message, originalBody, log)

        /* XML parsen                                                         */
        def root = new XmlSlurper().parseText(originalBody)

        /* Je nach Root-Element entscheiden, welcher Verarbeitungspfad läuft  */
        switch(root.name()) {
            case 'ProductMDMBulkReplicateRequestMessage':
                /* Gesamtes Bulk-Dokument, nur Validierung durchführen         */
                filterAndValidateProducts(root, log)
                break

            case 'Product':
                /* Ein einzelnes Produkt – Mapping erzeugen                    */
                def productJson = mapProduct(root, message, log)
                message.setBody(productJson)
                savePayloadAsAttachment(log, 'MappedProductJSON', productJson)
                break

            case 'Plant':
                /* Ein einzelnes Werk – Mapping erzeugen                       */
                def plantJson   = mapPlant(root, message, log)
                message.setBody(plantJson)
                savePayloadAsAttachment(log, 'MappedPlantJSON', plantJson)
                break

            default:
                throw new IllegalStateException("Unbekannter Root-Knoten: ${root.name()}")
        }

        return message

    } catch(Exception e) {
        /* zentrales Error-Handling                                           */
        handleError(originalBody, e, log)
        return message    // wird durch handleError eigentlich nie erreicht
    }
}

/* =========================================================================
 *  Modul: Header & Property Handling
 * ------------------------------------------------------------------------- */
@SuppressWarnings('GrMethodMayBeStatic')
void setHeadersAndProperties(Message message, String body, def log) {
    /* Versuche vorhandene Werte zu lesen, sonst 'placeholder' verwenden      */
    def xml = new XmlSlurper().parseText(body)
    def headerNode = xml.'**'.find { it.name() == 'MessageHeader' }

    /* Properties laut Aufgabenstellung                                      */
    setPropertyIfAbsent(message, 'requestUser' ,               'placeholder')
    setPropertyIfAbsent(message, 'requestPassword' ,           'placeholder')
    setPropertyIfAbsent(message, 'requestURL' ,                'placeholder')
    setPropertyIfAbsent(message, 'RecipientBusinessSystemID_config', 'Icertis_BUY')

    /* Werte aus XML                                                          */
    message.setProperty('SenderBusinessSystemID',
            headerNode?.SenderBusinessSystemID?.text() ?: 'placeholder')
    message.setProperty('RecipientBusinessSystemID_payload',
            headerNode?.RecipientBusinessSystemID?.text() ?: 'placeholder')

    /* Bei Produkt/Werk Kontext weitere Properties setzen                     */
    if(xml.name() == 'Product') {
        message.setProperty('ProductGroupInternalID',
                xml.ProductGroupInternalID?.text() ?: 'placeholder')
        message.setProperty('ProductInternalID',
                xml.ProductInternalID?.text() ?: 'placeholder')
    }

    if(xml.name() == 'Plant') {
        message.setProperty('PlantID',
                xml.PlantID?.text() ?: 'placeholder')
    }
    savePayloadAsAttachment(log, 'HeaderPropertyState', message.getProperties().toString())
}

/* Hilfsfunktion zum komfortablen Setzen eines Properties                   */
@SuppressWarnings('GrMethodMayBeStatic')
void setPropertyIfAbsent(Message msg, String key, String defaultVal) {
    if(!msg.getProperty(key)) {
        msg.setProperty(key, defaultVal)
    }
}

/* =========================================================================
 *  Modul: Produkt-Validierung
 * ------------------------------------------------------------------------- */
@SuppressWarnings('GrMethodMayBeStatic')
List filterAndValidateProducts(def root, def log) {

    def products = root.'**'.findAll { it.name() == 'Product' }

    /* Filter für Produktnummer > 999                                         */
    def validProducts = products.findAll { p ->
        extractNumeric(p.ProductInternalID.text()) > 999
    }

    if(validProducts.isEmpty()) {
        throw new IllegalArgumentException('Keine Produkte mit Nummer > 999 gefunden!')
    }

    savePayloadAsAttachment(log,
            'ValidProductsCount',
            "Es wurden ${validProducts.size()} gültige Produkte gefunden.")

    return validProducts
}

/* Extrahiert die Zahl aus einer alphanumerischen ID (z.B. PROD12345 => 12345)*/
@SuppressWarnings('GrMethodMayBeStatic')
int extractNumeric(String anyId) {
    def num = anyId?.replaceAll('[^0-9]', '')
    return num ? num.toInteger() : 0
}

/* =========================================================================
 *  Modul: Mapping – Product
 * ------------------------------------------------------------------------- */
@SuppressWarnings('GrMethodMayBeStatic')
String mapProduct(def productNode, Message message, def log) {

    savePayloadAsAttachment(log, 'ProductBeforeMapping', productNode.toString())

    /* Typ-Code in sprechenden Wert umsetzen                                  */
    def typeLookup = [ 'FG':'FinishedGood' ]
    def srcType    = productNode.ProductTypeCode.text()
    def mappedType = typeLookup.get(srcType?.toUpperCase(), srcType)

    /* DeletedIndicator negieren -> isActive                                  */
    def deleted    = productNode.DeletedIndicator.text()?.toLowerCase() == 'true'
    def isActive   = (!deleted).toString()

    def builder = new JsonBuilder()
    builder {
        Data {
            ICMProductCode            productNode.ProductInternalID.text()
            ICMProductTypeExternalID {
                DisplayValue          mappedType
            }
            ICMProductGroupCode {
                DisplayValue          productNode.ProductGroupInternalID.text()
            }
            ICMExternalSourceSystem   message.getProperty('SenderBusinessSystemID')
            isActive                  isActive
        }
    }

    return builder.toPrettyString()
}

/* =========================================================================
 *  Modul: Mapping – Plant
 * ------------------------------------------------------------------------- */
@SuppressWarnings('GrMethodMayBeStatic')
String mapPlant(def plantNode, Message message, def log) {

    savePayloadAsAttachment(log, 'PlantBeforeMapping', plantNode.toString())

    def builder = new JsonBuilder()
    builder {
        Data {
            ICMPlantID {
                DisplayValue plantNode.PlantID.text()
            }
            ICMExternalId {
                DisplayValue message.getProperty('ProductInternalID') ?: 'placeholder'
            }
        }
    }
    return builder.toPrettyString()
}

/* =========================================================================
 *  Modul: Logging
 * ------------------------------------------------------------------------- */
@SuppressWarnings('GrMethodMayBeStatic')
void savePayloadAsAttachment(def msgLog, String name, String payload) {
    msgLog?.addAttachmentAsString(name, payload ?: '', 'text/plain')
}

/* =========================================================================
 *  Modul: Zentrales Error-Handling
 * ------------------------------------------------------------------------- */
@SuppressWarnings('GrMethodMayBeStatic')
void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def msg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(msg, e)
}