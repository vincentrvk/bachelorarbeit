/****************************************************************************************
 *  CPI – Groovy-Script:  Business Partner – Inbound Mapping & Context Preparation
 *
 *  Aufgaben:
 *   – Prüfungen & Vorbereitungen (Properties / Header)
 *   – Business-Partner-Validierung  (InternalID > 999)
 *   – Mapping XML-BP → JSON (gem. Vorgaben)
 *   – Logging an allen relevanten Stellen (als Message-Attachment)
 *   – Zentrales, wiederverwendbares Error-Handling
 *
 *  HINWEIS:
 *  Dieses Skript ist so konzipiert, dass es entweder im Bulk-Kontext (noch ohne Splitter)
 *  oder – vorzugsweise – nach einem XML-Splitter (1 BP je Aufruf) ausgeführt werden kann.
 *  Sollte es im Bulk-Kontext ausgeführt werden, wird der erste gültige Business-Partner
 *  (InternalID > 999) verarbeitet. – In einem Splitter-Szenario liegt ohnehin nur ein
 *  <BusinessPartner>-Knoten im Body vor.
 *
 *  Autor:  AI-Assistant (Senior-Integration-Developer)
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.nio.charset.StandardCharsets

/* =====================================================
 *   Einstiegspunkt (CPI Message-API)
 * ====================================================*/
Message processData(Message message) {

    // Original-Payload sichern (für Error-Handling & Logging)
    final String originalBody = message.getBody(String)
    final def   msgLog       = messageLogFactory?.getMessageLog(message)

    try {
        /*------------------------------------------------
         *  1)  Context-Vorbereitung (Header / Properties)
         *-----------------------------------------------*/
        prepareContext(message, originalBody, msgLog)

        /*------------------------------------------------
         *  2)  XML einlesen
         *-----------------------------------------------*/
        def xml = new XmlSlurper().parseText(originalBody)

        /*------------------------------------------------
         *  3)  Passende Business-Partner ermitteln
         *-----------------------------------------------*/
        def bpNodes = xml.BusinessPartner.findAll { bp ->
            bp?.InternalID?.text()?.isInteger() && bp.InternalID.toInteger() > 999
        }

        // Validierung gem. Anforderung
        if (!bpNodes || bpNodes.isEmpty()) {
            throw new IllegalStateException(
                "Keine Business Partner mit InternalID > 999 im Eingangs-Payload gefunden.")
        }

        // In einem Splitter-Szenario existiert lediglich ein BP-Node
        def bpNode = bpNodes[0]

        /*------------------------------------------------
         *  4)  Business-Partner-Mapping
         *-----------------------------------------------*/
        def bpJson = mapBusinessPartner(bpNode, message, msgLog)

        /*------------------------------------------------
         *  5)  Ergebnis in Message-Body zurückliefern
         *-----------------------------------------------*/
        message.setBody(bpJson, String)

        return message

    } catch (Exception e) {
        // Zentrales Error-Handling
        handleError(originalBody, e, msgLog)
        return message    // (unreachable – handleError wirft Exception)
    }
}

/* =====================================================
 *                FUNKTIONEN
 * ====================================================*/

/*-------------------------------------------------------
 *  prepareContext(...)
 *  – Ermittelt / setzt alle benötigten Properties & Header
 *------------------------------------------------------*/
void prepareContext(Message msg, String body, def msgLog) {

    // Aktuelle Header / Properties (falls schon vorhanden)
    Map<String, Object> props  = msg.getProperties()
    Map<String, Object> hdrs   = msg.getHeaders()

    // XML zur Ableitung einzelner Werte
    def xmlRoot = new XmlSlurper().parseText(body)

    // Absender-System
    String senderSys = xmlRoot?.MessageHeader?.SenderBusinessSystemID?.text()?.trim()
    senderSys = senderSys ?: (props['SenderBusinessSystemID'] ?: hdrs['SenderBusinessSystemID'] ?: 'placeholder')

    /*--- Properties -----------------------------------*/
    msg.setProperty('requestUser'                    , props['requestUser' ] ?: hdrs['requestUser' ] ?: 'placeholder')
    msg.setProperty('requestPassword'                , props['requestPassword'] ?: hdrs['requestPassword'] ?: 'placeholder')
    msg.setProperty('requestURL'                     , props['requestURL'] ?: hdrs['requestURL'] ?: 'placeholder')
    msg.setProperty('RecipientBusinessSystemID_config', 'Icertis_BUY')
    // Sender-System kommt aus Payload oder Kontext
    msg.setProperty('SenderBusinessSystemID', senderSys)

    /*--- Logging --------------------------------------*/
    logPayload(msgLog, '01_PreparedContext', "Header & Properties wurden gesetzt.")
}

/*-------------------------------------------------------
 *  validateBusinessPartner(...)
 *  – Zusätzliche semantische Prüfungen (placeholder)
 *------------------------------------------------------*/
void validateBusinessPartner(def bpNode) {
    // Platzhalter für spätere, detaillierte Validierung (aktuell erledigt durch Auswahl)
}

/*-------------------------------------------------------
 *  mapBusinessPartner(...)
 *  – Führt das XML→JSON Mapping durch
 *------------------------------------------------------*/
String mapBusinessPartner(def bp, Message msg, def msgLog) {

    logPayload(msgLog, '02_Before_BP_Mapping', bp.toString())

    /*----------  Name-Ermittlung  ---------------------*/
    String givenName     = bp.Common?.Person?.Name?.GivenName?.text()?.trim()
    String familyName    = bp.Common?.Person?.Name?.FamilyName?.text()?.trim()
    String orgName       = bp.Common?.Organisation?.Name?.FirstLineName?.text()?.trim()

    String destName

    if (familyName) {
        // Familienname vorhanden  →  Given + Family (ohne Delimiter)
        destName = (givenName ?: '') + familyName
    } else {
        // Ansonsten Organisationsname
        destName = orgName ?: ''
    }

    /*--- Properties für Folge-Schritte ----------------*/
    msg.setProperty('SupplierCode',  bp.InternalID.text())
    msg.setProperty('SupplierName',  destName)

    /*----------  isActive -----------------------------*/
    // Eingehendes DeletedIndicator negieren (case-insensitive)
    def deletedIndicator = bp.DeletedIndicator?.text()?.toLowerCase()
    String isActive      = (deletedIndicator == 'true') ? 'false' : 'true'

    /*----------  syncRequired -------------------------*/
    def roleCodes = bp.Role.collect { it.RoleCode?.text()?.trim() }.findAll { it }

    /*----------  JSON Aufbauen ------------------------*/
    def builder = new JsonBuilder()
    builder {
        Data {
            ICMExternalId            bp.InternalID.text()
            Name                     destName
            ICMExternalSourceSystem  msg.getProperty('SenderBusinessSystemID')
            isActive                 isActive
            syncRequired             roleCodes
        }
    }

    String jsonResult = builder.toPrettyString()

    logPayload(msgLog, '03_After_BP_Mapping', jsonResult)

    return jsonResult
}

/*-------------------------------------------------------
 *  logPayload(...)
 *  – Fügt Payloads als Attachment hinzu (Monitoring)
 *------------------------------------------------------*/
void logPayload(def msgLog, String title, String content) {
    msgLog?.addAttachmentAsString(title, content, 'text/plain')
}

/*-------------------------------------------------------
 *  handleError(...)
 *  – Zentrales Error-Handling gem. Vorgabe
 *------------------------------------------------------*/
void handleError(String body, Exception e, def msgLog) {
    msgLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}