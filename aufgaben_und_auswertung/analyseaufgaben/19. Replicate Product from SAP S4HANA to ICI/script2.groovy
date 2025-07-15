/******************************************************************************
 * Skript:  S4/HANA  ->  ICI (Sell-Side) – Produkt-Masterdaten
 * Autor :  Senior-Integration-Developer (Groovy / SAP CI)
 * Datum :  18.06.2025
 *
 * Dieses Skript erfüllt sämtliche in der Aufgabenstellung beschriebenen
 * Anforderungen (Modularität, Mapping, Logging, Error-Handling, Validierung).
 *
 * ACHTUNG:
 * • Das Skript ist „stand-alone“ konzipiert und kann als Groovy-Script-Step
 *   in einer SAP Cloud-Integration IFlow verwendet werden.
 * • messageLogFactory wird von der Laufzeit automatisch injiziert.
 ******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonBuilder
import java.nio.charset.StandardCharsets

// =========================  Haupteinstieg  ==================================
Message processData(final Message message) {

    /* Ursprüngliches Payload sichern, um es im Fehlerfall als Attachment
       bereitstellen zu können */
    final String originalPayload = message.getBody(String)

    // MessageLog holen (kann null sein, z. B. in Testumgebungen)
    final def messageLog = messageLogFactory?.getMessageLog(message)

    try {
        // Header & Properties auslesen / setzen
        final Map<String, Object> props = determinePropertiesAndHeaders(message, messageLog)

        // Logging – Eingangs­payload
        logPayload(messageLog, 'IncomingPayload', originalPayload, 'text/xml')

        // Produkte splitten
        final List products = splitProducts(originalPayload)

        // Validierung der Produktliste
        final List validProducts = products.findAll { validateProduct(it, messageLog) }
        if (validProducts.isEmpty()) {
            throw new IllegalStateException('Kein Produkt mit Produktnummer > 999 gefunden – Abbruch.')
        }

        // Verarbeitung jedes gültigen Produkts
        validProducts.each { prodNode ->
            final String prodId = prodNode.ProductInternalID.text()

            /* ==============  1. Existenzprüfung (GET)  ===================== */
            logPayload(messageLog, "PreGET_${prodId}", '', 'text/plain')
            final boolean exists = checkIfProductExists(prodId, props, messageLog)
            logPayload(messageLog, "PostGET_${prodId}", "EXISTS=${exists}", 'text/plain')

            /* ==============  2. Mapping (XML -> JSON)  ===================== */
            final String mappedJson = mapProduct(prodNode)
            logPayload(messageLog, "MappedJSON_${prodId}", mappedJson, 'application/json')

            /* ==============  3. CREATE / UPDATE  =========================== */
            if (exists) {
                logPayload(messageLog, "PreUPDATE_${prodId}", mappedJson, 'application/json')
                updateProduct(mappedJson, props, messageLog)
                logPayload(messageLog, "PostUPDATE_${prodId}", 'UPDATE OK', 'text/plain')
            } else {
                logPayload(messageLog, "PreCREATE_${prodId}", mappedJson, 'application/json')
                createProduct(mappedJson, props, messageLog)
                logPayload(messageLog, "PostCREATE_${prodId}", 'CREATE OK', 'text/plain')
            }

            /* ==============  4. Aktivieren  ================================ */
            logPayload(messageLog, "PreACTIVATE_${prodId}", '', 'text/plain')
            activateProduct(prodId, props, messageLog)
            logPayload(messageLog, "PostACTIVATE_${prodId}", 'ACTIVATE OK', 'text/plain')
        }

        // Optionale Abschlussnachricht
        message.setBody('Verarbeitung erfolgreich abgeschlossen.')

        return message

    } catch (Exception e) {
        /* Fehlerbehandlung */
        handleError(originalPayload, e, messageLog)
        return message   // wird wegen RuntimeException nicht erreicht
    }
}

// ======================  Funktionsbibliothek  ==============================

/**
 * Bestimmt alle benötigten Header & Properties und legt fehlende Werte
 * mit „placeholder“ an.
 */
private Map<String, Object> determinePropertiesAndHeaders(
        final Message msg, final def mLog) {

    final Map<String, Object> props = [:]

    /* Liste der erwarteten Properties / Header  (Schlüssel = CPI-Property,
       Wert = Fallback) */
    final Map expected = [
            requestUser                   : 'placeholder',
            requestPassword               : 'placeholder',
            requestURL                    : 'placeholder',
            RecipientBusinessSystemID_config : 'Icertis_SELL',
            RecipientBusinessSystemID_payload: 'placeholder',
            SenderBusinessSystemID        : 'placeholder'
    ]

    expected.each { key, fallback ->
        def value = msg.getProperty(key)
        if (value == null) {
            value = msg.getHeader(key, String) ?: fallback
            msg.setProperty(key, value)   // Property nachziehen
        }
        props[key] = value
    }

    mLog?.addAttachmentAsString('ResolvedProperties',
            new JsonBuilder(props).toPrettyString(), 'application/json')

    return props
}

/**
 * Splittet das Ursprungs-XML und liefert eine Liste von „Product“-Nodes
 * (groovy.util.Node).
 */
private List splitProducts(final String xmlString) {
    def root = new XmlSlurper().parseText(xmlString)
    /* Namensraum agnostisch per Wildcard („*“) */
    return root.'**'.findAll { it.name() == 'Product' }
}

/**
 * Prüft, ob die Produktnummer > 999 ist.
 * Bei ungültigem Produkt wird es übersprungen, aber es wird geloggt.
 */
private boolean validateProduct(final def productNode, final def mLog) {

    final String prodId = productNode.ProductInternalID.text()
    final Matcher matcher = prodId =~ /(\d+)/
    final int number = matcher ? matcher[0][1].toInteger() : -1

    final boolean ok = number > 999
    if (!ok) {
        mLog?.addAttachmentAsString(
                "SkippedProduct_${prodId}",
                "Produkt übersprungen – Nummer <= 999",
                'text/plain')
    }
    return ok
}

/**
 * Führt die Existenzprüfung per GET-Call aus.
 * Liefert true, falls ein Produkt-Objekt im Antwort-Body gefunden wird.
 */
private boolean checkIfProductExists(
        final String prodId, final Map props, final def mLog) {

    final String urlStr = "${props.requestURL}?${URLEncoder.encode(prodId, 'UTF-8')}"
    final HttpURLConnection conn = openConnection(urlStr, 'GET', props)

    final int rc = conn.responseCode
    final String responseBody = conn.inputStream?.getText(StandardCharsets.UTF_8.name())

    mLog?.addAttachmentAsString(
            "GET_Response_${prodId}_RC_${rc}", responseBody ?: '', 'application/json')

    return responseBody?.toUpperCase()?.contains('PRODUCT')
}

/**
 * Ruft die ICI-Schnittstelle /Create auf.
 */
private void createProduct(
        final String jsonBody, final Map props, final def mLog) {

    final String urlStr = "${props.requestURL}/Create"
    final HttpURLConnection conn = openConnection(urlStr, 'POST', props)

    writeBody(conn, jsonBody)
    readAndLogResponse(conn, "CREATE_Response", mLog)
}

/**
 * Ruft die ICI-Schnittstelle /Update auf.
 */
private void updateProduct(
        final String jsonBody, final Map props, final def mLog) {

    final String urlStr = "${props.requestURL}/Update"
    final HttpURLConnection conn = openConnection(urlStr, 'POST', props)

    writeBody(conn, jsonBody)
    readAndLogResponse(conn, "UPDATE_Response", mLog)
}

/**
 * Ruft die ICI-Schnittstelle /Activate auf.
 */
private void activateProduct(
        final String prodId, final Map props, final def mLog) {

    final String urlStr = "${props.requestURL}/${URLEncoder.encode(prodId, 'UTF-8')}/Activate"
    final HttpURLConnection conn = openConnection(urlStr, 'POST', props)

    writeBody(conn, '')   // kein Body nötig
    readAndLogResponse(conn, "ACTIVATE_Response", mLog)
}

/**
 * Erstellt das JSON-Payload gem. Mapping-Spezifikation.
 */
private String mapProduct(final def productNode) {

    final String deleted = productNode.DeletedIndicator.text()
    final boolean deletedBool = deleted?.equalsIgnoreCase('true')

    def json = new JsonBuilder()
    json {
        Data {
            ICMProductName       productNode.Description.Description.text()
            ICMProductType       productNode.ProductTypeCode.text()
            ICMProductCode       productNode.ProductInternalID.text()
            isActive             (!deletedBool).toString()
            ICMProductCategoryName {
                DisplayValue productNode.ProductGroupInternalID.text()
            }
        }
    }
    return json.toPrettyString()
}

// ========================  Hilfsfunktionen  ================================

/* Öffnet eine HTTP-Verbindung mit Basic-Auth */
private HttpURLConnection openConnection(
        final String urlStr, final String method, final Map props) {

    final HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod(method)
    conn.setRequestProperty('Authorization', 'Basic ' +
            "${props.requestUser}:${props.requestPassword}".bytes.encodeBase64().toString())
    conn.setRequestProperty('Content-Type', 'application/json')
    conn.setDoOutput(true)
    conn
}

/* Schreibt einen Body in die Verbindung */
private void writeBody(final HttpURLConnection conn, final String body) {
    if (body) {
        conn.outputStream.withWriter('UTF-8') { it << body }
    } else {
        conn.connect()
    }
}

/* Liest Antwort & loggt sie */
private void readAndLogResponse(
        final HttpURLConnection conn, final String attachName, final def mLog) {

    final int rc = conn.responseCode
    final String resp = (rc >= 200 && rc < 300)
            ? conn.inputStream?.getText('UTF-8')
            : conn.errorStream?.getText('UTF-8')

    mLog?.addAttachmentAsString("${attachName}_RC_${rc}", resp ?: '', 'application/json')
}

/* Zentrale Attach-Logging-Methode */
private void logPayload(final def mLog, final String name,
                        final String payload, final String mime) {
    mLog?.addAttachmentAsString(name, payload ?: '', mime)
}

/* Fehlerbehandlung – wirft RuntimeException weiter */
private void handleError(final String body, final Exception e, final def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    final String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}