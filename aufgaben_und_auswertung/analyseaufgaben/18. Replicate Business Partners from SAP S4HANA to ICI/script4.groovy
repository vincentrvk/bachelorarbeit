/***************************************************************************************************
 * Groovy-Skript zur Integration von S/4 HANA Business Partnern in ICI (Sell Side)
 * Autor:   Senior-Integration Developer
 * Version: 1.0
 *
 * WICHTIG
 * • Das Skript ist modular aufgebaut – jede fachliche Aufgabe ist in einer dedizierten Funktion
 *   gekapselt.
 * • Alle Funktionen enthalten deutschsprachige Kommentare.
 * • Fehler werden zentral in handleError(..) behandelt. Der verursachende Payload wird als
 *   Attachment mitgegeben, sodass dieser im CPI-Monitoring eingesehen werden kann.
 * • Vor / nach jedem Mapping und vor / nach jedem externen API-Call wird der jeweilige Payload
 *   als Attachment geloggt (logPayload(..)).
 ***************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput
import java.nio.charset.StandardCharsets
import java.util.Base64
import groovy.xml.*

/* ============================== Haupteinstieg ================================================ */
Message processData(Message message) {

    /* MessageLog erzeugen (wird von mehreren Funktionen benötigt) */
    def messageLog = messageLogFactory.getMessageLog(message)

    try {

        /* 1. Header & Properties vorbereiten */
        def ctx = prepareContextValues(message)

        /* 2. Eingehenden Payload sichern und loggen */
        def inBody = message.getBody(String) ?: ''
        logPayload(messageLog, 'Incoming_XML_BusinessPartner', inBody, 'text/xml')

        /* 3. XML des einzelnen BusinessPartners parsen */
        def bpNode = new XmlSlurper().parseText(inBody)

        /* 4. BusinessPartner validieren */
        validateBusinessPartner(bpNode, messageLog, inBody)

        /* 5. Mapping XML → JSON für ICI-API (vorher / nachher loggen) */
        logPayload(messageLog, 'Before_BusinessPartner_Mapping', inBody, 'text/xml')
        def jsonBody = mapBusinessPartner(bpNode)
        logPayload(messageLog, 'After_BusinessPartner_Mapping', jsonBody, 'application/json')

        /* 6. Prüfen, ob Business Partner bereits existiert */
        def existsResponse = apiCheckExists(ctx, messageLog)
        logPayload(messageLog, 'Response_CheckExists', existsResponse, 'application/json')
        def bpExists = existsResponse?.trim()

        /* 7. Entsprechend Create oder Update aufrufen */
        if (bpExists) {
            apiUpdateBusinessPartner(ctx, jsonBody, messageLog)  // UPDATE
        } else {
            apiCreateBusinessPartner(ctx, jsonBody, messageLog)  // CREATE
        }

        /* 8. Abschließend Business Partner aktiv setzen */
        apiActivateBusinessPartner(ctx, jsonBody, messageLog)

        /* 9. Ergebnis in Message zurückgeben */
        message.setBody(jsonBody)
        return message

    } catch (Exception e) {
        /* Zentrales Error-Handling */
        handleError(message.getBody(String) ?: '', e, messageLog)
        return message   // wird bei Exception nie erreicht, aber von CPI verlangt
    }
}

/* ==============================  Hilfsfunktionen  ============================================ */

/**
 * Liest notwendige Header / Properties, Payload-Werte aus und stellt sie im Kontext (Map) bereit.
 * Fehlt ein Wert, wird 'placeholder' gesetzt.
 */
private Map prepareContextValues(Message message) {

    // XML-Payload zum Auslesen gewisser Werte
    def bodyXml = message.getBody(String) ?: ''
    def xml      = new XmlSlurper().parseText(bodyXml)

    /* Properties ermitteln */
    def ctx = [
            requestUser     : (message.getProperty('requestUser')     ?: 'placeholder'),
            requestPassword : (message.getProperty('requestPassword') ?: 'placeholder'),
            requestURL      : (message.getProperty('requestURL')      ?: 'placeholder'),
            recipientCfg    : 'Icertis_SELL',
            senderSystemId  : (xml?.MessageHeader?.SenderBusinessSystemID?.text()      ?: 'placeholder'),
            recipientPaylId : (xml?.MessageHeader?.RecipientBusinessSystemID?.text()  ?: 'placeholder'),
            supplierCode    : (xml?.InternalID?.text() ?: 'placeholder')
    ]

    /* SupplierCode auch als Property im Message-Objekt ablegen (wird u.U. von nachfolgenden
       Groovy-/IFlow-Schritten genutzt) */
    message.setProperty('SupplierCode', ctx.supplierCode)

    return ctx
}

/**
 * Prüft gem. Vorgabe: Nur InternalIDs > 999 sind gültig. Andernfalls Exception.
 */
private void validateBusinessPartner(def bpNode, def msgLog, String rawXml) {

    def internalIdRaw = bpNode?.InternalID?.text() ?: ''
    def numericPart   = internalIdRaw.replaceAll('\\D', '')  // alles außer Ziffern entfernen
    def numericVal    = numericPart ? numericPart as Long : 0L

    if (numericVal <= 999L) {
        handleError(rawXml,
                new IllegalArgumentException("BusinessPartner ${internalIdRaw} fällt durch Validierungsregel – InternalID <= 999."), msgLog)
    }
}

/**
 * Führt das Mapping von XML → JSON gem. Business-Logik durch.
 */
private String mapBusinessPartner(def bp) {

    /* NaturalPersonIndicator ermitteln */
    def isNaturalPerson = (bp?.Common?.NaturalPersonIndicator?.text() == 'true')

    /* ICMContractingPartyType ermitteln */
    def contractingPartyType = isNaturalPerson ? 'Individual' : 'Customer'

    /* ICMExternalId ermitteln */
    def icmExternalId = isNaturalPerson ?
            (bp?.Customer?.InternalID?.text() ?: '') :
            (bp?.InternalID?.text()           ?: '')

    /* Name bestimmen */
    def name
    if (isNaturalPerson) {
        def given     = bp?.Common?.Person?.Name?.GivenName?.text()            ?: ''
        def family    = bp?.Common?.Person?.Name?.FamilyName?.text()           ?: ''
        def addFamily = bp?.Common?.Person?.Name?.AdditionalFamilyName?.text() ?: ''
        name = [given, family, addFamily].findAll { it }.join(' ').trim()
    } else {
        name = bp?.Common?.Organisation?.Name?.FirstLineName?.text() ?: ''
    }

    /* syncRequired – alle RoleCodes als Array */
    def roleCodes = bp?.Role?.collect { it?.RoleCode?.text() }?.findAll { it } ?: []
    if (!roleCodes) {
        // gem. Zielschema Pflichtfeld → zumindest 'true' eintragen
        roleCodes = ['true']
    }

    /* CountryCode */
    def countryCode = bp?.AddressInformation?.Address?.PostalAddress?.CountryCode?.text() ?: ''

    def target = [
            Data: [
                    ICMContractingPartyType: contractingPartyType,
                    ICMExternalId          : icmExternalId,
                    Name                   : name,
                    syncRequired           : roleCodes,
                    ICMCountryCode         : [
                            DisplayValue: countryCode
                    ]
            ]
    ]
    return JsonOutput.prettyPrint(JsonOutput.toJson(target))
}

/**
 * Ruft die ICI-API auf, um zu prüfen, ob der BusinessPartner bereits existiert.
 * Liefert den Response-Body zurück (leer → nicht vorhanden | gefüllt → vorhanden).
 */
private String apiCheckExists(Map ctx, def msgLog) {

    def url = "${ctx.requestURL}?${ctx.supplierCode}"
    logPayload(msgLog, 'Request_CheckExists', '', 'text/plain', url)

    def conn = new URL(url).openConnection()
    conn.setRequestMethod('GET')
    conn.setRequestProperty('Authorization', buildBasicAuthHeader(ctx.requestUser, ctx.requestPassword))

    def resp = conn.inputStream.getText(StandardCharsets.UTF_8.name())
    return resp
}

/**
 * Führt einen UPDATE-Call aus.
 */
private void apiUpdateBusinessPartner(Map ctx, String jsonBody, def msgLog) {

    def url = "${ctx.requestURL}/Update"
    logPayload(msgLog, 'Request_Update', jsonBody, 'application/json', url)
    callPost(url, jsonBody, ctx, msgLog, 'Response_Update')
}

/**
 * Führt einen CREATE-Call aus.
 */
private void apiCreateBusinessPartner(Map ctx, String jsonBody, def msgLog) {

    def url = "${ctx.requestURL}/Create"
    logPayload(msgLog, 'Request_Create', jsonBody, 'application/json', url)
    callPost(url, jsonBody, ctx, msgLog, 'Response_Create')
}

/**
 * Setzt den Business Partner auf aktiv.
 */
private void apiActivateBusinessPartner(Map ctx, String jsonBody, def msgLog) {

    def url = "${ctx.requestURL}/${ctx.supplierCode}/Activate"
    logPayload(msgLog, 'Request_Activate', jsonBody, 'application/json', url)
    callPost(url, jsonBody, ctx, msgLog, 'Response_Activate')
}

/**
 * Generische Methode für POST-Calls.
 */
private void callPost(String url, String jsonBody, Map ctx, def msgLog, String logNameAfter) {

    HttpURLConnection conn
    try {
        conn = (HttpURLConnection) new URL(url).openConnection()
        conn.setRequestMethod('POST')
        conn.setDoOutput(true)
        conn.setRequestProperty('Content-Type', 'application/json')
        conn.setRequestProperty('Authorization', buildBasicAuthHeader(ctx.requestUser, ctx.requestPassword))

        conn.outputStream.withWriter(StandardCharsets.UTF_8.name()) { it << jsonBody }
        def responseTxt = conn.inputStream.getText(StandardCharsets.UTF_8.name())
        logPayload(msgLog, logNameAfter, responseTxt, 'application/json', url)
    } finally {
        conn?.disconnect()
    }
}

/**
 * Erstellt den Basic-Auth-Header.
 */
private String buildBasicAuthHeader(String user, String pwd) {

    def token = "${user}:${pwd}".getBytes(StandardCharsets.UTF_8)
    return "Basic ${Base64.encoder.encodeToString(token)}"
}

/**
 * Fügt Payloads als Attachment im MessageLog hinzu.
 * type = Mimetype (z.B. 'text/xml', 'application/json')
 * extInfo = optionale Zusatzinfo für Dateinamen (z.B. URL)
 */
private void logPayload(def msgLog, String name, String payload, String type, String extInfo = '') {

    try {
        if (msgLog) {
            def attName = extInfo ? "${name} (${extInfo})" : name
            msgLog.addAttachmentAsString(attName, payload, type)
        }
    } catch (Exception ignore) {
        /* Logging-Fehler sollen nicht den Hauptablauf stören */
    }
}

/**
 * Zentrale Fehlerbehandlung – wirft RuntimeException mit aussagekräftiger Meldung.
 * Der auslösende Payload wird immer als Attachment im MessageLog abgelegt.
 */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}