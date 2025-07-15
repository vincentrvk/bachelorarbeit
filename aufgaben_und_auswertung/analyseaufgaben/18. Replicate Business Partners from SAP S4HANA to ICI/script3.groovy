/*****************************************************************************
 *  Groovy-Skript:  S4/HANA  →  ICI  (Sell Side) – Business Partner Sync
 *-----------------------------------------------------------------------------
 *  Autor  :  CPI-Integration-Team
 *  Version: 1.0
 *
 *  Beschreibung:
 *  1.   Liest den eingehenden XML-Payload, splittet auf einzelne
 *       <BusinessPartner>-Einträge und verarbeitet jeden Partner separat.
 *  2.   Validiert gem. Vorgaben (InternalID > 999).
 *  3.   Prüft via REST-GET, ob der Business Partner bereits existiert.
 *       • Existiert     → UPDATE-Call
 *       • Existiert nicht → CREATE-Call
 *  4.   Setzt den Business Partner anschließend auf „Active“.
 *  5.   Mapping XML → JSON gem. Mapping-Spezifikation.
 *  6.   Durchgängiges Logging (Attachments) & Error-Handling.
 *****************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern

Message processData(Message message) {

    /*--------- Globale Initialisierungen ----------------------------------*/
    def messageLog = messageLogFactory.getMessageLog(message)
    String originalBody = message.getBody(String)                               // Originalpayload sichern 
    try {

        /*--------- 1. Properties & Header setzen ---------------------------*/
        setDynamicValues(message, originalBody, messageLog)

        /*--------- 2. XML parsen & BusinessPartner listen ------------------*/
        def bpList = new XmlSlurper().parseText(originalBody)
                                      .BusinessPartner
                                      .findAll { node ->
            isValidInternalId(node.InternalID.text())
        }

        /* -- Fehler, wenn keiner die Validierung besteht -------------------*/
        if (bpList.isEmpty()) {
            throw new IllegalStateException("Kein Business Partner mit InternalID > 999 gefunden.")
        }

        /*--------- 3. Verarbeitung jedes Business Partners -----------------*/
        bpList.each { bpNode ->

            /*--- SupplierCode / Einzel-Properties -------------------------*/
            String supplierCode = bpNode.InternalID.text()
            message.setProperty("SupplierCode", supplierCode)

            /*--- 3.1 Existenzprüfung --------------------------------------*/
            boolean exists = checkBusinessPartnerExists(message, supplierCode, messageLog)

            /*--- 3.2 Mapping XML → JSON -----------------------------------*/
            logAttachment(messageLog, "BeforeMapping_${supplierCode}", groovy.xml.XmlUtil.serialize(bpNode))
            String jsonPayload = mapBusinessPartnerToJson(bpNode)
            logAttachment(messageLog, "AfterMapping_${supplierCode}", jsonPayload)

            /*--- 3.3 REST-Call (CREATE oder UPDATE) ------------------------*/
            if (exists) {
                callUpdateBusinessPartner(message, jsonPayload, messageLog)
            } else {
                callCreateBusinessPartner(message, jsonPayload, messageLog)
            }

            /*--- 3.4 Aktivieren -------------------------------------------*/
            callActivateBusinessPartner(message, supplierCode, messageLog)
        }

    } catch (Exception e) {
        handleError(originalBody, e, messageLog)                                // Zentrales Error-Handling
    }
    return message
}

/*============================================================================
 *                      F U N K T I O N S B L O C K
 *===========================================================================*/

/*  setDynamicValues ---------------------------------------------------------
 *  Befüllt Properties & Header – vorhandene Werte werden beibehalten.
 */
void setDynamicValues(Message msg, String body, def log) {
    Map<String, Object> props = msg.getProperties()
    Map<String, Object> heads = msg.getHeaders()

    // Helper Closure
    def setIfMissing = { map, key, val ->
        if (!map.containsKey(key) || map[key] == null) {
            map[key] = val
        }
    }

    // Header - aktuell keine Vorgabe außer evtl. spätere Erweiterung
    // Properties
    setIfMissing(props, "requestUser",                 "placeholder")
    setIfMissing(props, "requestPassword",             "placeholder")
    setIfMissing(props, "requestURL",                  "placeholder")
    setIfMissing(props, "RecipientBusinessSystemID_config", "Icertis_SELL")

    // Werte aus Payload übernehmen
    def xmlRoot = new XmlSlurper().parseText(body)
    setIfMissing(props, "RecipientBusinessSystemID_payload",
                 xmlRoot.MessageHeader.RecipientBusinessSystemID.text() ?: "placeholder")
    setIfMissing(props, "SenderBusinessSystemID",
                 xmlRoot.MessageHeader.SenderBusinessSystemID.text() ?: "placeholder")

    msg.setProperties(props)
    msg.setHeaders(heads)
}

/*  isValidInternalId --------------------------------------------------------
 *  Validiert InternalID anhand Regel (> 999)
 */
boolean isValidInternalId(String internalId) {
    if (!internalId) { return false }
    String digits = internalId.replaceAll(/[^0-9]/, '')
    return digits.isInteger() && digits.toInteger() > 999
}

/*  mapBusinessPartnerToJson -------------------------------------------------
 *  Wandelt eine BusinessPartner-Node in das geforderte JSON-Format.
 */
String mapBusinessPartnerToJson(def bpNode) {

    boolean isNatural = bpNode.Common.NaturalPersonIndicator.text().toBoolean()

    /*------ Zielfeld-Ermittlung -----------------------------------------*/
    String icmType   = isNatural ? "Customer (Person)" : "Customer"
    String externalId= isNatural ?
                       bpNode.Customer.InternalID.text() :
                       bpNode.InternalID.text()

    String name      = isNatural ?
            [bpNode.Common.Person.Name.GivenName.text(),
             bpNode.Common.Person.Name.FamilyName.text(),
             bpNode.Common.Person.Name.AdditionalFamilyName.text()]
             .findAll { it }                                                   // Leere Felder entfernen
             .join(" ").trim() :
            bpNode.Common.Organisation.Name.FirstLineName.text()

    List<String> syncRequired = [bpNode.Role.RoleCode.text() ? "true" : "false"]

    String countryCode = bpNode.AddressInformation.Address.PostalAddress.CountryCode.text()

    /*------ JSON-Generierung -------------------------------------------*/
    def builder = new JsonBuilder()
    builder {
        Data {
            ICMContractingPartyType  icmType
            ICMExternalId           externalId
            Name                    name
            syncRequired            syncRequired
            ICMCountryCode {
                DisplayValue        countryCode
            }
        }
    }
    return builder.toPrettyString()
}

/*  checkBusinessPartnerExists ----------------------------------------------
 *  REST-GET → liefert true (existiert) | false (nicht vorhanden)
 */
boolean checkBusinessPartnerExists(Message msg, String supplierCode, def log) {

    String url     = "${msg.getProperty('requestURL')}/${supplierCode}"
    String user    = msg.getProperty('requestUser')
    String pass    = msg.getProperty('requestPassword')
    String auth    = "${user}:${pass}".bytes.encodeBase64().toString()

    logAttachment(log, "GET_Exists_Before_${supplierCode}", url)

    def conn = new URL(url).openConnection()
    conn.requestMethod       = "GET"
    conn.setRequestProperty("Authorization", "Basic ${auth}")

    int rc = conn.responseCode
    String respBody = rc == 200 ? conn.inputStream.getText(StandardCharsets.UTF_8.name())
                                : conn.errorStream?.getText(StandardCharsets.UTF_8.name())

    logAttachment(log, "GET_Exists_After_${supplierCode}", "HTTP ${rc}\n${respBody}")

    return (rc == 200 && respBody?.contains('"BusinessPartner"'))
}

/*  callUpdateBusinessPartner ------------------------------------------------
 *  Sendet POST /Update
 */
void callUpdateBusinessPartner(Message msg, String json, def log) {
    callPost(msg, "${msg.getProperty('requestURL')}/Update", json, "UPDATE", log)
}

/*  callCreateBusinessPartner ------------------------------------------------
 *  Sendet POST /Create
 */
void callCreateBusinessPartner(Message msg, String json, def log) {
    callPost(msg, "${msg.getProperty('requestURL')}/Create", json, "CREATE", log)
}

/*  callActivateBusinessPartner ---------------------------------------------
 *  POST /{SupplierCode}/Activate
 */
void callActivateBusinessPartner(Message msg, String supplierCode, def log) {
    callPost(msg,
             "${msg.getProperty('requestURL')}/${supplierCode}/Activate",
             "", "ACTIVATE", log)
}

/*  callPost -----------------------------------------------------------------
 *  Generische POST-Methode inkl. Logging.
 */
void callPost(Message msg, String url, String body, String tag, def log) {

    String user    = msg.getProperty('requestUser')
    String pass    = msg.getProperty('requestPassword')
    String auth    = "${user}:${pass}".bytes.encodeBase64().toString()

    logAttachment(log, "${tag}_Before_${url}", body ?: "<empty>")

    def conn = new URL(url).openConnection()
    conn.requestMethod       = "POST"
    conn.doOutput            = true
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("Authorization", "Basic ${auth}")

    if (body) { conn.outputStream.withWriter("UTF-8") { it << body } }

    int rc = conn.responseCode
    String respBody = rc >= 200 && rc < 300 ?
                      conn.inputStream.getText(StandardCharsets.UTF_8.name()) :
                      conn.errorStream?.getText(StandardCharsets.UTF_8.name())
    logAttachment(log, "${tag}_After_${url}", "HTTP ${rc}\n${respBody}")
}

/*  logAttachment ------------------------------------------------------------
 *  Fügt dem Message Log einen Anhang hinzu (Name, Payload)
 */
void logAttachment(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content ?: "<null>", "text/plain")
}

/*  handleError --------------------------------------------------------------
 *  Zentrales Error-Handling gem. Vorgabe
 */
void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im Business Partner Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}