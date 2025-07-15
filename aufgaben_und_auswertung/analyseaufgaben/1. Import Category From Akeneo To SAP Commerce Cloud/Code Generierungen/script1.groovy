/****************************************************************************************
 *  Skript:     Import Categories – Akeneo  ->  SAP Commerce Cloud
 *  Autor:      Senior Integration Developer
 *  Hinweis:    Dieses Skript ist für SAP Cloud Integration (CPI) geschrieben.
 *              Es erfüllt alle in der Aufgabenstellung beschriebenen Anforderungen.
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets
import java.util.Base64

// ======================================================================================
//  Haupteinstieg                                                                       |
// ======================================================================================
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    try {

        /* ---------------------------------------------------------------------------
         *  1. Header & Property Vorbereitung
         * ------------------------------------------------------------------------- */
        def ctx = prepareContext(message, messageLog)      // Map mit allen benötigten Werten

        /* ---------------------------------------------------------------------------
         *  2. Aufruf Auth-Token (Akeneo)
         * ------------------------------------------------------------------------- */
        String accessToken = callAkeneoAuthorize(ctx, messageLog)

        /* ---------------------------------------------------------------------------
         *  3. Aufruf Kategorien (Akeneo)
         * ------------------------------------------------------------------------- */
        List<String> categoryCodes = getCategories(ctx, accessToken, messageLog)

        /* ---------------------------------------------------------------------------
         *  4. Mapping JSON  ->  XML
         * ------------------------------------------------------------------------- */
        String xmlBody = mapCategoriesToXml(ctx, categoryCodes, messageLog)

        /* ---------------------------------------------------------------------------
         *  5. Versenden an Commerce Cloud
         * ------------------------------------------------------------------------- */
        sendCategoriesToCommerceCloud(ctx, xmlBody, messageLog)

        /* ---------------------------------------------------------------------------
         *  6. CPI-Nachricht vorbereiten (Body/Headers)
         * ------------------------------------------------------------------------- */
        message.setBody(xmlBody)
        message.setHeader("Content-Type", "application/xml")

        return message
    } catch (Exception e) {
        handleError(message.getBody(String) as String, e, messageLog)
        return message     // wird nie erreicht, da handleError RuntimeException wirft
    }
}

// ======================================================================================
//  1. Kontext-Vorbereitung                                                              |
// ======================================================================================
/**
 *  Liest alle benötigten Header & Properties aus der CPI-Nachricht.
 *  Fehlende Werte werden mit „placeholder“ ersetzt und sofort in die Nachricht geschrieben.
 *  Es wird eine Map mit allen Werten zurückgegeben, um Funktionsaufrufe zu vereinfachen.
 */
private Map prepareContext(Message message, def messageLog) {

    // Hilfsclosure: Hole Wert aus Header ODER Property, schreibe ihn zurück, setze ggf. placeholder
    def resolveValue = { String key, boolean isHeader ->
        def val = isHeader ? message.getHeader(key, String) : message.getProperty(key)
        if (val == null || val.toString().trim().isEmpty()) {
            val = "placeholder"
            if (isHeader)  { message.setHeader(key, val) }
            else           { message.setProperty(key, val) }
            messageLog?.addAttachmentAsString("PlaceholderInfo", "Key '${key}' wurde mit 'placeholder' befüllt.", "text/plain")
        }
        return val.toString()
    }

    // Header
    def ctx = [:]
    ctx.aknPIMClientSecret     = resolveValue('aknPIMClientSecret', true)
    ctx.X_PIM_URL              = resolveValue('X-PIM-URL', true)
    ctx.X_PIM_CLIENT_ID        = resolveValue('X-PIM-CLIENT-ID', true)
    ctx.AKN_PIM_Category_code  = resolveValue('AKN PIM Category code', true)

    // Properties
    ctx.catId              = resolveValue('catId', false)
    ctx.catVersion         = resolveValue('catVersion', false)
    ctx.akeneoGraphQlUrl   = resolveValue('AKN GraphQl URL', false)
    ctx.akeneoUsername     = resolveValue('akeneoUsername', false)
    ctx.akeneoPassword     = resolveValue('akeneoPassword', false)
    ctx.commerceUrl        = resolveValue('CommerceCloudURL', false)
    ctx.commerceUsername   = resolveValue('commerceUsername', false)
    ctx.commercePassword   = resolveValue('commercePassword', false)

    return ctx
}

// ======================================================================================
//  2. Akeneo Authorize                                                                  |
// ======================================================================================
/**
 *  Ruft über GraphQL das Access-Token von Akeneo ab.
 *  Bei Erfolg wird der Token-Wert zurückgegeben und zusätzlich als Header „X-PIM-TOKEN“
 *  in der CPI-Nachricht abgelegt.
 */
private String callAkeneoAuthorize(Map ctx, def messageLog) {
    String basicAuth = base64("${ctx.akeneoUsername}:${ctx.akeneoPassword}")

    // GraphQL-Query aufbauen
    String query = """query getToken{
                        token(username: "${ctx.akeneoUsername}",
                              password: "${ctx.akeneoPassword}",
                              clientId: "${ctx.X_PIM_CLIENT_ID}",
                              clientSecret: "${ctx.aknPIMClientSecret}") {
                          data { accessToken }
                        }
                      }""".replaceAll("\\s+", " ")  // Beautify

    String requestBody = JsonOutput.toJson([query: query])

    HttpURLConnection conn = openConnection(ctx.akeneoGraphQlUrl, "POST", [
            "Content-Type"  : "application/json",
            "X-PIM-URL"     : ctx.X_PIM_URL,
            "X-PIM-CLIENT-ID": ctx.X_PIM_CLIENT_ID,
            "Authorization" : "Basic ${basicAuth}"
    ])
    writeBody(conn, requestBody)

    String response = readResponse(conn)
    Map json = new JsonSlurper().parseText(response)
    String token = json?.data?.token?.data?.accessToken
    if (!token) {
        throw new RuntimeException("Kein Access-Token in Akeneo-Antwort gefunden: ${response}")
    }

    messageLog?.addAttachmentAsString("AkeneoAuthorizeResponse", response, "application/json")
    // Header für Folgeaufrufe
    messageLog.getMessage().setHeader("X-PIM-TOKEN", token)

    return token
}

// ======================================================================================
//  3. Kategorien abrufen                                                                 |
// ======================================================================================
/**
 *  Ruft per GraphQL die gewünschten Kategorien ab.
 *  Gibt eine Liste der Kategoriecodes zurück.
 */
private List<String> getCategories(Map ctx, String token, def messageLog) {

    String basicAuth = base64("${ctx.akeneoUsername}:${ctx.akeneoPassword}")

    String query = """query GetCategories{
                        categories(codes: "${ctx.AKN_PIM_Category_code}") {
                          items { code }
                        }
                      }""".replaceAll("\\s+", " ")

    String requestBody = JsonOutput.toJson([query: query])

    HttpURLConnection conn = openConnection(ctx.akeneoGraphQlUrl, "POST", [
            "Content-Type"  : "application/json",
            "X-PIM-URL"     : ctx.X_PIM_URL,
            "X-PIM-CLIENT-ID": ctx.X_PIM_CLIENT_ID,
            "X-PIM-TOKEN"   : token,
            "Authorization" : "Basic ${basicAuth}"
    ])
    writeBody(conn, requestBody)

    String response = readResponse(conn)
    Map json = new JsonSlurper().parseText(response)
    List items = json?.data?.categories?.items ?: []
    List<String> codes = items.collect { it.code as String }.findAll { it }

    if (codes.isEmpty()) {
        throw new RuntimeException("Keine Kategorien im Akeneo-Response gefunden: ${response}")
    }

    messageLog?.addAttachmentAsString("AkeneoCategoriesResponse", response, "application/json")
    return codes
}

// ======================================================================================
//  4. Mapping JSON -> XML                                                               |
// ======================================================================================
/**
 *  Wandelt die Liste der Kategorie-Codes in das gewünschte XML-Format um.
 */
private String mapCategoriesToXml(Map ctx, List<String> codes, def messageLog) {
    def writer = new StringWriter()
    def builder = new MarkupBuilder(writer)
    builder.mkp.xmlDeclaration(version: "1.0", encoding: "UTF-8")

    builder.Categories {
        codes.each { String codeVal ->
            Category {
                catalogVersion {
                    CatalogVersion {
                        catalog {
                            Catalog {
                                id(ctx.catId)
                                integrationKey(ctx.catId)
                            }
                        }
                        version(ctx.catVersion)
                        integrationKey(ctx.catVersion)
                    }
                }
                code(codeVal)
                integrationKey(codeVal)
            }
        }
    }

    String xml = writer.toString()
    messageLog?.addAttachmentAsString("MappedCategoriesXML", xml, "application/xml")
    return xml
}

// ======================================================================================
//  5. Versand an Commerce Cloud                                                         |
// ======================================================================================
/**
 *  Sendet das XML an Commerce Cloud per HTTP-POST (Basic Auth).
 *  Erfolgreiche Response-Codes (200-299) werden geloggt, andere als Fehler geworfen.
 */
private void sendCategoriesToCommerceCloud(Map ctx, String xmlBody, def messageLog) {

    String basicAuth = base64("${ctx.commerceUsername}:${ctx.commercePassword}")

    HttpURLConnection conn = openConnection(ctx.commerceUrl, "POST", [
            "Content-Type" : "application/xml",
            "Authorization": "Basic ${basicAuth}"
    ])
    writeBody(conn, xmlBody)

    int rc = conn.responseCode
    String resp = readResponse(conn)

    if (rc < 200 || rc >= 300) {
        throw new RuntimeException("Commerce Cloud-Fehler (HTTP ${rc}): ${resp}")
    }
    messageLog?.addAttachmentAsString("CommerceCloudResponse", resp, "text/plain")
}

// ======================================================================================
//  6. Hilfsfunktionen                                                                   |
// ======================================================================================
/**
 *  Erstellt und öffnet eine HttpURLConnection mit allen übergebenen Headern.
 */
private HttpURLConnection openConnection(String url, String method, Map<String, String> headers) {
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    conn.with {
        requestMethod = method
        doOutput      = true
        connectTimeout = 15000
        readTimeout    = 30000
        headers.each { k, v -> setRequestProperty(k, v) }
    }
    return conn
}

/** Schreibt Request-Body in eine HttpURLConnection */
private void writeBody(HttpURLConnection conn, String body) {
    conn.outputStream.withWriter("UTF-8") { it << body }
}

/** Liest den Response-Body (Error-Stream berücksichtigt) */
private String readResponse(HttpURLConnection conn) {
    InputStream is = conn.responseCode >= 400 ? conn.errorStream : conn.inputStream
    return is ? is.getText("UTF-8") : ""
}

/** Base64-Helper (UTF-8) */
private String base64(String value) {
    return Base64.encoder.encodeToString(value.getBytes(StandardCharsets.UTF_8))
}

// ======================================================================================
//  Fehlerbehandlung                                                                     |
// ======================================================================================
/**
 *  Zentrales Error-Handling gem. Vorgabe.
 *  Fügt den Payload als Attachment hinzu, schreibt Log-Nachricht und wirft RuntimeException.
 */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "", "text/plain")
    def errorMsg = "Fehler im Kategorien-Import: ${e.message}"
    messageLog?.setStringProperty("ErrorMessage", errorMsg)
    throw new RuntimeException(errorMsg, e)
}