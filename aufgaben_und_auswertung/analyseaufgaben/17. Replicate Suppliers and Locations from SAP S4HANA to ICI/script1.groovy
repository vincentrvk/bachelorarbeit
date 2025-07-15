/****************************************************************************************
 *  ICI – Business-Partner Integration (Buy-Side)
 *  ------------------------------------------------
 *  Groovy-Skript für SAP Cloud Integration
 *
 *  Autor  : Senior Integration Developer
 *  Version: 1.0
 *
 *  Dieses Skript erfüllt folgende Aufgaben:
 *    • Setzt benötigte Header & Properties
 *    • Validiert eingehende Business-Partner-Daten
 *    • Führt alle nötigen API-Calls zu ICI aus (Check-Exists, Create/Update, Activate)
 *    • Erstellt die dazugehörigen JSON-Payloads
 *    • Erstellt/Updatet zugehörige Adressen
 *    • Schreibt detaillierte Logs inkl. Payload-Attachments
 *    • Implementiert zentrales Error-Handling
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

// Haupteinstiegspunkt des Skriptes
Message processData(Message message) {
    final String originalBody = message.getBody(String) ?: ''
    def msgLog = messageLogFactory.getMessageLog(message)       // CPI-Monitoring-Log

    try {
        // Kontext vorbereiten (Header/Properties)
        setContext(message, msgLog)

        // Eingehenden Payload loggen
        logPayload(msgLog, 'IncomingPayload', originalBody, 'text/xml')

        // XML parsen
        def root        = new XmlSlurper().parseText(originalBody)
        def bpCandidates = root.BusinessPartner.findAll { it.InternalID.text().isInteger() && it.InternalID.text().toInteger() > 999 }

        // Validierung
        validatePresenceOfValidBP(bpCandidates, msgLog)

        // Alle gültigen Business-Partner verarbeiten
        bpCandidates.each { bp ->
            // Property für Folge-Schritte setzen
            message.setProperty('SupplierCode', bp.InternalID.text())

            // Mapping Business-Partner
            def bpJson      = mapBusinessPartner(bp, message, msgLog)
            def partnerPath = decideForCreateOrUpdate(bpJson, 'BusinessPartner', message, msgLog)

            // Create/Update Business-Partner
            invokePost("${message.getProperty('requestURL')}/${partnerPath}", bpJson, message, msgLog)

            // Aktiv-Status setzen
            invokePost("${message.getProperty('requestURL')}/${message.getProperty('SupplierCode')}/Activate", '{}', message, msgLog)

            // Adressen verarbeiten
            bp.AddressInformation.each { addr ->
                message.setProperty('AddressUUID', addr.UUID.text())
                def addrJson   = mapAddress(addr, message, msgLog)
                def addrPath   = decideForCreateOrUpdate(addrJson, 'AddressInformation', message, msgLog)
                invokePost("${message.getProperty('requestURL')}/${addrPath}", addrJson, message, msgLog)
            }
        }

        return message
    } catch (Exception ex) {
        handleError(originalBody, ex, msgLog)       // Zentrales Error-Handling
        return message                              // (wird durch handleError i.d.R. nicht erreicht)
    }
}

/* =============================================================================
   Funktions-Sektion
   ========================================================================== */

/* ********************************************************************************
 *  setContext
 *  Liest vorhandene Header/Properties oder setzt Default-Werte (placeholder)
 ******************************************************************************** */
void setContext(Message msg, def log) {
    // Hilfsclosure zum Ermitteln eines Wertes
    def fetchVal = { String key, def defaultVal ->
        msg.getProperty(key) ?: msg.getHeader(key, String) ?: defaultVal
    }

    // Pflicht-Properties (werden für API-Aufrufe benötigt)
    msg.setProperty('requestUser' , fetchVal('requestUser' , 'placeholder'))
    msg.setProperty('requestPassword', fetchVal('requestPassword', 'placeholder'))
    msg.setProperty('requestURL', fetchVal('requestURL', 'placeholder'))

    // Weitere Vorgaben aus Aufgabenstellung
    msg.setProperty('RecipientBusinessSystemID_config', 'Icertis_BUY')
    msg.setProperty('SenderBusinessSystemID', fetchVal('SenderBusinessSystemID', 'placeholder'))

    logPayload(log, 'ContextProperties', new JsonBuilder(msg.getProperties()).toPrettyString(), 'application/json')
}

/* ********************************************************************************
 *  validatePresenceOfValidBP
 *  Prüft, ob mindestens ein Business-Partner mit InternalID > 999 vorhanden ist
 ******************************************************************************** */
void validatePresenceOfValidBP(def bpList, def log) {
    if (!bpList || bpList.isEmpty()) {
        throw new IllegalStateException('Kein gültiger Business-Partner (InternalID > 999) im Payload gefunden!')
    }
    log?.addAttachmentAsString('Validation', "Gültige BP-Anzahl: ${bpList.size()}", 'text/plain')
}

/* ********************************************************************************
 *  mapBusinessPartner
 *  Erstellt die JSON-Struktur für einen Business-Partner laut Mapping-Vorgaben
 ******************************************************************************** */
String mapBusinessPartner(def bp, Message msg, def log) {
    // Namensbestimmung
    String givenName  = bp.Common?.Person?.Name?.GivenName?.text() ?: ''
    String familyName = bp.Common?.Person?.Name?.FamilyName?.text() ?: ''
    String orgName    = bp.Common?.Organisation?.Name?.FirstLineName?.text() ?: ''
    String supplierName = familyName ? "${givenName}${familyName}" : orgName

    // SupplierName als Property für Address-Mapping verfügbar machen
    msg.setProperty('SupplierName', supplierName)

    // isActive (Negation DeletedIndicator)
    def deletedIndicator = (bp.Common?.DeletedIndicator?.text() ?: 'false').toString().toLowerCase()
    def isActive         = (!deletedIndicator.toBoolean()).toString()

    // Rolle(n) ermitteln
    def roles = bp.Role.collect { it.RoleCode.text() }.findAll { it }          // Liste ohne leere Werte

    // JSON-Builder
    def builder = new JsonBuilder()
    builder {
        Data {
            ICMExternalId           bp.InternalID.text()
            Name                   supplierName
            ICMExternalSourceSystem msg.getProperty('SenderBusinessSystemID')
            isActive               isActive
            syncRequired           roles
        }
    }

    String jsonOut = builder.toPrettyString()
    logPayload(log, 'BP-Mapping-Result', jsonOut, 'application/json')
    return jsonOut
}

/* ********************************************************************************
 *  mapAddress
 *  Erstellt die JSON-Struktur für eine Adresse laut Mapping-Vorgaben
 ******************************************************************************** */
String mapAddress(def addr, Message msg, def log) {
    String jsonOut = new JsonBuilder().call {
        Data {
            ICMExternalId            addr.UUID.text()
            Name                     msg.getProperty('SupplierName')
            ICMAddressLine1          addr.Address?.PostalAddress?.HouseID?.text() ?: ''
            ICMExternalSourceSystem  msg.getProperty('SenderBusinessSystemID')
            ICMCountryCode           { DisplayValue 'DE' }                        // Konstante lt. Beispiel
            ICMICISupplierCode       { DisplayValue msg.getProperty('SupplierCode') }
        }
    }.toPrettyString()

    logPayload(log, 'Address-Mapping-Result', jsonOut, 'application/json')
    return jsonOut
}

/* ********************************************************************************
 *  decideForCreateOrUpdate
 *  Prüft via GET-Call, ob das Objekt bereits existiert – gibt Pfad für POST zurück
 ******************************************************************************** */
String decideForCreateOrUpdate(String payload, String objectTag, Message msg, def log) {
    String baseUrl = msg.getProperty('requestURL')
    String query   = objectTag == 'BusinessPartner' ?
            msg.getProperty('SupplierCode') : msg.getProperty('AddressUUID')

    String response = invokeGet(baseUrl, query, msg, log)
    boolean exists  = response?.contains(objectTag)

    logPayload(log, 'CheckExistsResponse', response, 'application/json')

    return exists ?
            (objectTag == 'BusinessPartner' ? 'Update'        : 'UpdateAddress') :
            (objectTag == 'BusinessPartner' ? 'Create'        : 'CreateAddress')
}

/* ********************************************************************************
 *  invokeGet
 *  Führt einen GET-Request mit Basic-Auth aus und gibt Response-String zurück
 ******************************************************************************** */
String invokeGet(String url, String query, Message msg, def log) {
    String fullUrl = "${url}?${URLEncoder.encode(query, StandardCharsets.UTF_8.toString())}"
    logPayload(log, 'GET-Request', fullUrl, 'text/plain')

    HttpURLConnection conn = (HttpURLConnection) new URL(fullUrl).openConnection()
    conn.setRequestMethod('GET')
    setBasicAuth(conn, msg)

    int status = conn.responseCode
    String response = conn.inputStream?.getText('UTF-8') ?: ''

    logPayload(log, "GET-Response-${status}", response, 'text/plain')
    return response
}

/* ********************************************************************************
 *  invokePost
 *  Führt einen POST-Request mit Basic-Auth aus
 ******************************************************************************** */
void invokePost(String url, String body, Message msg, def log) {
    logPayload(log, 'POST-Request-URL', url, 'text/plain')
    logPayload(log, 'POST-Request-Body', body, 'application/json')

    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    conn.setRequestMethod('POST')
    conn.doOutput = true
    conn.setRequestProperty('Content-Type', 'application/json')
    setBasicAuth(conn, msg)

    conn.outputStream.withWriter('UTF-8') { it << body }

    int status = conn.responseCode
    String response = conn.inputStream?.getText('UTF-8') ?: ''
    logPayload(log, "POST-Response-${status}", response, 'application/json')
}

/* ********************************************************************************
 *  setBasicAuth
 *  Fügt der Verbindung den Basic-Auth-Header hinzu
 ******************************************************************************** */
void setBasicAuth(HttpURLConnection conn, Message msg) {
    String auth = "${msg.getProperty('requestUser')}:${msg.getProperty('requestPassword')}"
    String enc  = auth.bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${enc}")
}

/* ********************************************************************************
 *  logPayload
 *  Hängt beliebige Inhalte als Attachment an das Message-Log an
 ******************************************************************************** */
void logPayload(def log, String name, String content, String mime) {
    log?.addAttachmentAsString(name, content ?: '', mime ?: 'text/plain')
}

/* ********************************************************************************
 *  handleError
 *  Zentrales Error-Handling – wirft RuntimeException weiter
 ******************************************************************************** */
void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}