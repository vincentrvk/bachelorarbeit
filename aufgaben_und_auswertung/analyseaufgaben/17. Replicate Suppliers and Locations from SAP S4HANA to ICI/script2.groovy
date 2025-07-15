/*****************************************************************************************
*  SAP Cloud Integration – Groovy-Skript                                                   *
*  Aufgabe:      Business-Partner & Adress-Synchronisation S/4-HANA → ICI (Buy-Side)      *
*  Autor:        ChatGPT (Senior-Developer)                                               *
*                                                                                         *
*  WICHTIG:                                                                               *
*  • Jede Funktion ist modular aufgebaut und deutsch kommentiert.                         *
*  • Vollständiges Error-Handling mit Log-Attachments.                                    *
*  • Payload-Logging vor / nach jedem Mapping sowie vor / nach jedem HTTP-Request.        *
*  • Keine globalen Variablen / Konstanten, keine unnötigen Imports.                      *
******************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput
import java.nio.charset.StandardCharsets
import javax.xml.parsers.*
import org.w3c.dom.*
import javax.xml.xpath.*

/* =========================================================================
   ===  H A U P T R O U T I N E  ==========================================
   ========================================================================= */
Message processData(Message message) {

    /* ---------------------------------------------------------------------
       1. Initiales Setup
    --------------------------------------------------------------------- */
    def bodyString  = message.getBody(String) ?: ''
    def messageLog  = messageLogFactory.getMessageLog(message)
    addLog(messageLog, 'Incoming-Payload', bodyString)

    try {
        /* --------------------------------------------------------------
           2. Header / Properties befüllen (falls nicht vorhanden)
        -------------------------------------------------------------- */
        setHeadersAndProperties(message, bodyString, messageLog)

        /* --------------------------------------------------------------
           3. Business-Partner extrahieren & validieren
        -------------------------------------------------------------- */
        def bpList = extractValidBusinessPartner(bodyString, messageLog)
        if (bpList.isEmpty()) {
            throw new IllegalStateException('Kein Business-Partner mit InternalID > 999 gefunden!')
        }

        /* --------------------------------------------------------------
           4. Verarbeitung je Business-Partner
        -------------------------------------------------------------- */
        bpList.each { bpNode ->
            processBusinessPartner(bpNode, message, messageLog)
        }

    } catch (Exception e) {
        handleError(bodyString, e, messageLog)          // definiert weiter unten
    }

    return message
}

/* =========================================================================
   ===  F U N K T I O N E N  ===============================================
   ========================================================================= */

/*-------------------------------------------------------------------------*
 *  Funktion: setHeadersAndProperties                                      *
 *-------------------------------------------------------------------------*/
void setHeadersAndProperties(Message msg, String body, def log) {
    /*  Header / Properties aus Message lesen oder mit Platzhalter füllen   */
    def props  = msg.getProperties()
    def hdrs   = msg.getHeaders()

    def slurper = new XmlSlurper().parseText(body)
    def senderId = slurper?.MessageHeader?.SenderBusinessSystemID?.text() ?: 'placeholder'

    /* Property-Defaulting */
    msg.setProperty('requestUser',               props.requestUser      ?: 'placeholder')
    msg.setProperty('requestPassword',           props.requestPassword  ?: 'placeholder')
    msg.setProperty('requestURL',                props.requestURL       ?: 'placeholder')
    msg.setProperty('RecipientBusinessSystemID_config', 'Icertis_BUY')
    msg.setProperty('SenderBusinessSystemID',    senderId)

    addLog(log, 'HeaderProperty-Init', "Properties und Header wurden gesetzt.")
}

/*-------------------------------------------------------------------------*
 *  Funktion: extractValidBusinessPartner                                   *
 *-------------------------------------------------------------------------*/
List extractValidBusinessPartner(String xml, def log) {
    /*  Liefert Liste aller Business-Partner-Knoten mit InternalID > 999   */
    def slurper = new XmlSlurper().parseText(xml)
    def valid   = slurper.BusinessPartner.findAll { 
                        (it.InternalID.text() as Integer) > 999 
                  }
    addLog(log, 'BP-Validation', "Gefundene gültige Business-Partner: ${valid.size()}")
    return valid
}

/*-------------------------------------------------------------------------*
 *  Funktion: processBusinessPartner                                        *
 *-------------------------------------------------------------------------*/
void processBusinessPartner(def bp, Message msg, def log) {

    /* SupplierCode Property für Folge-Aufrufe */
    def supplierCode = bp.InternalID.text()
    msg.setProperty('SupplierCode', supplierCode)

    /* ------------------------------------------------------------------
       1) Prüfen, ob BP existiert
    ------------------------------------------------------------------ */
    def exists = checkBusinessPartnerExists(msg, log)

    /* ------------------------------------------------------------------
       2) Mapping Business-Partner → JSON
    ------------------------------------------------------------------ */
    def bpJson = mapBusinessPartner(bp, msg, log)

    /* ------------------------------------------------------------------
       3) CREATE oder UPDATE
    ------------------------------------------------------------------ */
    if (exists) {
        updateBusinessPartner(bpJson, msg, log)
    } else {
        createBusinessPartner(bpJson, msg, log)
    }

    /* ------------------------------------------------------------------
       4) Aktivschalten
    ------------------------------------------------------------------ */
    setActiveStatus(msg, log)

    /* ------------------------------------------------------------------
       5) Adressen-Verarbeitung
    ------------------------------------------------------------------ */
    bp.AddressInformation.each { addr ->
        processAddress(addr, msg, log)
    }
}

/*-------------------------------------------------------------------------*
 *  Funktion: processAddress                                                *
 *-------------------------------------------------------------------------*/
void processAddress(def addrNode, Message msg, def log) {

    def addressUUID = addrNode.UUID.text()
    msg.setProperty('AddressUUID', addressUUID)

    boolean exists = checkAddressExists(msg, log)

    /* Mapping */
    def addrJson = mapAddress(addrNode, msg, log)

    /* CREATE or UPDATE */
    if (exists) {
        updateAddress(addrJson, msg, log)
    } else {
        createAddress(addrJson, msg, log)
    }
}

/*-------------------------------------------------------------------------*
 *  Funktion: mapBusinessPartner                                            *
 *-------------------------------------------------------------------------*/
String mapBusinessPartner(def bp, Message msg, def log) {
    addLog(log, 'Mapping-BP-Input', groovy.xml.XmlUtil.serialize(bp))

    /* Namensfindung nach Regel */
    def given   = bp.Common?.Person?.Name?.GivenName?.text()      ?: ''
    def family  = bp.Common?.Person?.Name?.FamilyName?.text()     ?: ''
    def orgName = bp.Common?.Organisation?.Name?.FirstLineName?.text() ?: ''

    def name = family ? "${given}${family}" : orgName
    msg.setProperty('SupplierName', name)    // Property für Adressen-Mapping

    def data = [
        ICMExternalId          : bp.InternalID.text(),
        Name                   : name,
        ICMExternalSourceSystem: msg.getProperty('SenderBusinessSystemID'),
        isActive               : (!bp.DeletedIndicator.text().toString()
                                  .equalsIgnoreCase('true')).toString(),
        syncRequired           : bp.Role.collect { it.RoleCode.text() }
    ]

    def json = JsonOutput.toJson([Data: data])
    addLog(log, 'Mapping-BP-Output', JsonOutput.prettyPrint(json))
    return json
}

/*-------------------------------------------------------------------------*
 *  Funktion: mapAddress                                                    *
 *-------------------------------------------------------------------------*/
String mapAddress(def addr, Message msg, def log) {
    addLog(log, 'Mapping-Address-Input', groovy.xml.XmlUtil.serialize(addr))

    def data = [
        ICMExternalId          : addr.UUID.text(),
        Name                   : msg.getProperty('SupplierName'),
        ICMAddressLine1        : addr.Address?.PostalAddress?.HouseID?.text(),
        ICMExternalSourceSystem: msg.getProperty('SenderBusinessSystemID'),
        ICMCountryCode         : [DisplayValue: 'DE'],
        ICMICISupplierCode     : [DisplayValue: msg.getProperty('SupplierCode')]
    ]

    def json = JsonOutput.toJson([Data: data])
    addLog(log, 'Mapping-Address-Output', JsonOutput.prettyPrint(json))
    return json
}

/*-------------------------------------------------------------------------*
 *  Funktion: checkBusinessPartnerExists (GET)                              *
 *-------------------------------------------------------------------------*/
boolean checkBusinessPartnerExists(Message msg, def log) {
    def url  = "${msg.getProperty('requestURL')}?${msg.getProperty('SupplierCode')}"
    def resp = execHttp('GET', url, null, msg, log, 'Check-BP')
    return resp?.contains('"Data"')          // einfache Heuristik
}

/*-------------------------------------------------------------------------*
 *  Funktion: updateBusinessPartner (POST)                                  *
 *-------------------------------------------------------------------------*/
void updateBusinessPartner(String payload, Message msg, def log) {
    def url = "${msg.getProperty('requestURL')}/Update"
    execHttp('POST', url, payload, msg, log, 'UPDATE-BP')
}

/*-------------------------------------------------------------------------*
 *  Funktion: createBusinessPartner (POST)                                  *
 *-------------------------------------------------------------------------*/
void createBusinessPartner(String payload, Message msg, def log) {
    def url = "${msg.getProperty('requestURL')}/Create"
    execHttp('POST', url, payload, msg, log, 'CREATE-BP')
}

/*-------------------------------------------------------------------------*
 *  Funktion: setActiveStatus (POST)                                        *
 *-------------------------------------------------------------------------*/
void setActiveStatus(Message msg, def log) {
    def url = "${msg.getProperty('requestURL')}/${msg.getProperty('SupplierCode')}/Activate"
    execHttp('POST', url, '', msg, log, 'SET-ACTIVE')
}

/*-------------------------------------------------------------------------*
 *  Funktion: checkAddressExists (GET)                                      *
 *-------------------------------------------------------------------------*/
boolean checkAddressExists(Message msg, def log) {
    def url  = "${msg.getProperty('requestURL')}?${msg.getProperty('AddressUUID')}"
    def resp = execHttp('GET', url, null, msg, log, 'Check-Address')
    return resp?.contains('"Data"')
}

/*-------------------------------------------------------------------------*
 *  Funktion: updateAddress (POST)                                          *
 *-------------------------------------------------------------------------*/
void updateAddress(String payload, Message msg, def log) {
    def url = "${msg.getProperty('requestURL')}/UpdateAddress"
    execHttp('POST', url, payload, msg, log, 'UPDATE-Address')
}

/*-------------------------------------------------------------------------*
 *  Funktion: createAddress (POST)                                          *
 *-------------------------------------------------------------------------*/
void createAddress(String payload, Message msg, def log) {
    def url = "${msg.getProperty('requestURL')}/CreateAddress"
    execHttp('POST', url, payload, msg, log, 'CREATE-Address')
}

/*-------------------------------------------------------------------------*
 *  Funktion: execHttp – generischer HTTP-Aufruf                            *
 *-------------------------------------------------------------------------*/
String execHttp(String method,
                String urlString,
                String payload,
                Message msg,
                def log,
                String context) {

    addLog(log, "${context}-Request-URL", urlString)
    if (payload != null) {
        addLog(log, "${context}-Request-Body", payload)
    }

    def connection
    String responseBody = ''
    try {
        connection = new URL(urlString).openConnection()
        connection.setRequestMethod(method)
        connection.setRequestProperty('Authorization',
                'Basic ' + "${msg.getProperty('requestUser')}:${msg.getProperty('requestPassword')}"
                        .bytes.encodeBase64().toString())
        connection.setRequestProperty('Content-Type', 'application/json')
        connection.doInput  = true
        connection.doOutput = (method == 'POST')

        if (method == 'POST' && payload != null) {
            connection.outputStream.withWriter('UTF-8') { it << payload }
        }

        int rc = connection.responseCode
        responseBody = connection.inputStream?.getText(StandardCharsets.UTF_8.name()) ?: ''
        addLog(log, "${context}-Response-Code", rc.toString())
        addLog(log, "${context}-Response-Body", responseBody)

    } catch (Exception e) {
        handleError(payload ?: '', e, log)
    } finally {
        connection?.disconnect()
    }
    return responseBody
}

/*-------------------------------------------------------------------------*
 *  Funktion: addLog – Attachment Logging                                   *
 *-------------------------------------------------------------------------*/
void addLog(def log, String name, String content) {
    /*  Jeder Log-Eintrag wird als Attachment mitgegeben                    */
    log?.addAttachmentAsString(name, content ?: '', 'text/plain')
}

/*-------------------------------------------------------------------------*
 *  Funktion: handleError – zentrales Error-Handling                        *
 *-------------------------------------------------------------------------*/
def handleError(String body, Exception e, def messageLog) {
    // Verwendung des vorgegebenen Snippets
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}