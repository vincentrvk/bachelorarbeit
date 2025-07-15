/***************************************************************************************************
 *  Groovy-Skript :  S/4 HANA  →  ICI  (Buy-Side)  – Business Partner Integration
 *  Autor        :  Senior-Integration-Developer (SAP CI)
 *  Beschreibung :  Vollständige Umsetzung sämtlicher Vorgaben der Aufgabenstellung.
 *
 *  Wichtig      :  • KEINE globalen Variablen / Konstanten
 *                  • Modularer Aufbau gem. Spezifikation
 *                  • Vollständiges Error-Handling + Logging (Attachments)
 ***************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.net.HttpURLConnection
import java.nio.charset.StandardCharsets

class BPSync {

    /* =================================================================================================
       === Public Entry-Point ============================================================================
       =============================================================================================== */
    static Message execute(Message message) {

        def messageLog = messageLogFactory.getMessageLog(message)

        try {

            /* 1. Kontext-Initialisierung ----------------------------------------------------------------*/
            setContextProperties(message, messageLog)

            /* 2. Persistiere ursprünglichen Payload -----------------------------------------------------*/
            logAttachment(message, messageLog, 'IncomingPayload', message.getBody(String))

            /* 3. Business Partner - Loop ----------------------------------------------------------------*/
            def xml      = new XmlSlurper().parseText(message.getBody(String))
            def bpsValid = xml.BusinessPartner.findAll { bp -> validateBusinessPartner(bp, message, messageLog) }

            if (bpsValid.isEmpty()) {
                throw new IllegalStateException('Kein Business Partner mit InternalID > 999 gefunden.')
            }

            bpsValid.each { bpNode -> processBusinessPartner(bpNode, message, messageLog) }

            return message

        } catch (Exception e) {
            /* zentrales Error-Handling */
            handleError(message.getBody(String), e, messageLog)
            return message   // unreachable – RuntimeException wird geworfen
        }
    }

    /* =================================================================================================
       === Kontext-/Header-Pflege =======================================================================
       =============================================================================================== */
    private static void setContextProperties(Message msg, def log) {

        /* vorhandene Werte übernehmen – ansonsten placeholder                                           */
        def props = msg.getProperties()

        putIfAbsent(msg, 'requestUser',                    props['requestUser']                    ?: 'placeholder')
        putIfAbsent(msg, 'requestPassword',                props['requestPassword']                ?: 'placeholder')
        putIfAbsent(msg, 'requestURL',                     props['requestURL']                     ?: 'placeholder')

        putIfAbsent(msg, 'RecipientBusinessSystemID_config','Icertis_BUY')
        putIfAbsent(msg, 'SenderBusinessSystemID',         props['SenderBusinessSystemID']         ?: 'placeholder')
        /* RecipientBusinessSystemID_payload und SupplierCode werden dynamisch je BP gesetzt            */

        log.addAttachmentAsString('ContextAfterInit', new groovy.json.JsonBuilder(msg.getProperties()).toPrettyString(), 'application/json')
    }

    private static void putIfAbsent(Message msg, String key, Object value) {
        if (msg.getProperty(key) == null) {
            msg.setProperty(key, value)
        }
    }

    /* =================================================================================================
       === Business Partner – Orchestrierung ============================================================
       =============================================================================================== */
    private static void processBusinessPartner(def bpNode, Message msg, def log) {

        /* SupplierCode Property aktualisieren */
        msg.setProperty('SupplierCode', (bpNode.InternalID.text() ?: 'placeholder'))

        /* 4. GET – Existenzprüfung Business Partner ---------------------------------------------------*/
        def exists = checkEntityExists(msg, log, msg.getProperty('SupplierCode'))

        /* 5. Mapping Business Partner -----------------------------------------------------------------*/
        def bpPayload = mapBusinessPartner(bpNode, msg, log)

        /* 6. POST – Create/Update Business Partner ----------------------------------------------------*/
        String url = "${msg.getProperty('requestURL')}/${ exists ? 'Update' : 'Create' }"
        callPost(url, bpPayload, msg, log)

        /* 7. POST – Activate Business Partner ---------------------------------------------------------*/
        callPost("${msg.getProperty('requestURL')}/${msg.getProperty('SupplierCode')}/Activate", '{}', msg, log)

        /* 8. Adresse(n) verarbeiten -------------------------------------------------------------------*/
        bpNode.AddressInformation.each { addrNode ->
            processAddress(addrNode, msg, log)
        }
    }

    /* =================================================================================================
       === Adress-Orchestrierung ========================================================================
       =============================================================================================== */
    private static void processAddress(def addrNode, Message msg, def log) {

        /* Property AddressUUID setzen */
        String addrUuid = addrNode.UUID.text() ?: 'placeholder'
        msg.setProperty('AddressUUID', addrUuid)

        /* 9. GET – Existenzprüfung Adresse -----------------------------------------------------------*/
        def exists = checkEntityExists(msg, log, addrUuid)

        /* 10. Mapping Adresse ------------------------------------------------------------------------*/
        def addrPayload = mapAddress(addrNode, msg, log)

        /* 11. POST – Create/Update Adresse -----------------------------------------------------------*/
        String url = "${msg.getProperty('requestURL')}/${ exists ? 'UpdateAddress' : 'CreateAddress' }"
        callPost(url, addrPayload, msg, log)
    }

    /* =================================================================================================
       === Validierung Business Partner ================================================================
       =============================================================================================== */
    private static boolean validateBusinessPartner(def bpNode, Message msg, def log) {

        def id = bpNode.InternalID.text() ?: '0'
        boolean valid = id.isInteger() && id.toInteger() > 999

        if (!valid) {
            log.addAttachmentAsString("BP_Skipped_${id}", bpNode.toString(), 'text/xml')
        }
        return valid
    }

    /* =================================================================================================
       === Mapping Business Partner =====================================================================
       =============================================================================================== */
    private static String mapBusinessPartner(def bpNode, Message msg, def log) {

        /* Name-Aufbereitung   -----------------------------------------------------------------------*/
        String givenName  = bpNode.Common.Person?.Name?.GivenName?.text()
        String familyName = bpNode.Common.Person?.Name?.FamilyName?.text()
        String orgName    = bpNode.Common.Organisation?.Name?.FirstLineName?.text()

        String nameResult = (familyName ?: '').trim()
        if (nameResult) {
            nameResult = "${givenName ?: ''}${familyName}"
        } else {
            nameResult = orgName ?: ''
        }
        msg.setProperty('SupplierName', nameResult)

        /* isActive negiertes DeletedIndicator -------------------------------------------------------*/
        def deleted = bpNode.DeletedIndicator.text().toLowerCase()
        String isActive = (deleted == 'true') ? 'false' : 'true'

        /* JSON-Erstellung ---------------------------------------------------------------------------*/
        def json = new JsonBuilder()
        json {
            Data {
                ICMExternalId            bpNode.InternalID.text()
                Name                    nameResult
                ICMExternalSourceSystem msg.getProperty('SenderBusinessSystemID')
                isActive                isActive
                syncRequired            [ bpNode.Role?.RoleCode?.text() ?: '' ]
            }
        }

        /* Logging */
        logAttachment(msg, log, "BP_Mapping_${bpNode.InternalID.text()}", json.toPrettyString())

        return json.toString()
    }

    /* =================================================================================================
       === Mapping Adresse =============================================================================
       =============================================================================================== */
    private static String mapAddress(def addrNode, Message msg, def log) {

        def json = new JsonBuilder()
        json {
            Data {
                ICMExternalId            addrNode.UUID.text()
                Name                    msg.getProperty('SupplierName')
                ICMAddressLine1         (addrNode.Address?.PostalAddress?.HouseID?.text() ?: '')
                ICMExternalSourceSystem msg.getProperty('SenderBusinessSystemID')
                ICMCountryCode {         DisplayValue 'DE'          }
                ICMICISupplierCode {     DisplayValue msg.getProperty('SupplierCode') }
            }
        }
        logAttachment(msg, log, "ADDR_Mapping_${addrNode.UUID.text()}", json.toPrettyString())
        return json.toString()
    }

    /* =================================================================================================
       === HTTP-Hilfsfunktionen (GET / POST) ============================================================
       =============================================================================================== */
    private static boolean checkEntityExists(Message msg, def log, String queryValue) {

        String url = "${msg.getProperty('requestURL')}?${URLEncoder.encode(queryValue, 'UTF-8')}"
        def conn   = buildConnection(url, 'GET', msg)

        /* Logging - vor Aufruf */
        logAttachment(msg, log, "HTTP_GET_Request_${queryValue}", '')

        int rc = conn.responseCode
        String responseBody = rc == 200 ? conn.inputStream.getText('UTF-8') : ''

        /* Logging - nach Aufruf */
        logAttachment(msg, log, "HTTP_GET_Response_${queryValue}", responseBody ?: "HTTP ${rc}")

        return responseBody?.contains('"BusinessPartner"') || responseBody?.contains('"AddressInformation"')
    }

    private static void callPost(String url, String payload, Message msg, def log) {

        /* Logging - vor Aufruf */
        logAttachment(msg, log, "HTTP_POST_Request_${url}", payload)

        def conn = buildConnection(url, 'POST', msg)
        conn.setRequestProperty('Content-Type', 'application/json')
        conn.doOutput = true
        conn.outputStream.withWriter('UTF-8') { it << payload }

        int rc = conn.responseCode
        String resp = (rc == HttpURLConnection.HTTP_OK || rc == HttpURLConnection.HTTP_CREATED)
                ? conn.inputStream.getText('UTF-8')
                : conn.errorStream?.getText('UTF-8') ?: ''

        /* Logging - nach Aufruf */
        logAttachment(msg, log, "HTTP_POST_Response_${url}", resp ?: "HTTP ${rc}")
    }

    private static HttpURLConnection buildConnection(String urlStr, String method, Message msg) {
        def url  = new URL(urlStr)
        def conn = (HttpURLConnection) url.openConnection()
        conn.requestMethod = method
        conn.setRequestProperty('Authorization', basicAuthHeader(msg))
        conn.connectTimeout = 30000
        conn.readTimeout    = 30000
        return conn
    }

    private static String basicAuthHeader(Message msg) {
        def user = msg.getProperty('requestUser') ?: ''
        def pwd  = msg.getProperty('requestPassword') ?: ''
        def token = "${user}:${pwd}".getBytes(StandardCharsets.UTF_8).encodeBase64().toString()
        return "Basic ${token}"
    }

    /* =================================================================================================
       === Logging-Utility =============================================================================
       =============================================================================================== */
    private static void logAttachment(Message msg, def log, String name, String content) {
        log?.addAttachmentAsString(name, content ?: '', 'text/plain')
    }

    /* =================================================================================================
       === Error-Handling (zentral) =====================================================================
       =============================================================================================== */
    private static void handleError(String body, Exception e, def messageLog) {
        messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
        def errorMsg = "Fehler im BP-Integrations-Skript: ${e.message}"
        throw new RuntimeException(errorMsg, e)
    }
}

/* ==== CPI-Standard-Entry ============================================================================*/
import com.sap.gateway.ip.core.customdev.util.Message as SAPMessage
SAPMessage processData(SAPMessage message) {
    return BPSync.execute(message)
}