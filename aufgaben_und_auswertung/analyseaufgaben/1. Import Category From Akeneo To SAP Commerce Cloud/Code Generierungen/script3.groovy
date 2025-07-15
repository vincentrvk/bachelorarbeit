/****************************************************************************************
 * Akeneo → SAP Commerce Cloud – Kategorien-Integrationsskript
 *
 * Dieses Skript wird als Groovy-Script-Step in der SAP Cloud Integration verwendet.
 * Es erledigt folgende Aufgaben:
 * 1.   Prüfen/Befüllen der notwendigen Header & Properties                 (setDefaults)
 * 2.   Authorisierung bei Akeneo GraphQL-API                                (callAkeneoAuthorize)
 * 3.   Abfrage der gewünschten Kategorien                                   (callAkeneoGetCategories)
 * 4.   Mapping von JSON → Commerce-XML                                      (mapCategoriesToCommerceXml)
 * 5.   Versenden der Kategorien an die Commerce Cloud                       (callSendToCommerce)
 * 
 * Jede Funktion besitzt eigenes Error-Handling; zentral wird im catch-Block von
 * processData(..) handleError(..) gerufen, welches den Payload als Attachment anhängt
 * und die Exception erneut wirft.
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.xml.MarkupBuilder

// === Haupteinstieg ===============================================================
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    // Ursprünglicher Payload für späteres Error-Handling
    def originalBody = message.getBody(String) ?: ""

    try {
        setDefaults(message, messageLog)
        callAkeneoAuthorize(message, messageLog)
        callAkeneoGetCategories(message, messageLog)
        mapCategoriesToCommerceXml(message, messageLog)
        callSendToCommerce(message, messageLog)
        return message
    } catch (Exception e) {
        handleError(originalBody, e, messageLog)
    }
    // wird nie erreicht, handleError wirft Exception
    return message
}

// === 1. Header & Property Defaulting =============================================
/**
 * Befüllt fehlende Header und Properties mit "placeholder".
 */
def setDefaults(Message msg, def log) {
    // Obligatorische Header
    [
        'aknPIMClientSecret',
        'X-PIM-URL',
        'X-PIM-CLIENT-ID',
        'Content-Type',
        'AKN PIM Category code'
    ].each { h ->
        if (msg.getHeader(h, String) == null) {
            msg.setHeader(h, h == 'Content-Type' ? 'application/json' : 'placeholder')
            log?.addAttachmentAsString("Header-Default-$h", msg.getHeader(h, String), "text/plain")
        }
    }

    // Obligatorische Properties
    [
        'catId',
        'catVersion',
        'AKN GraphQl URL',
        'akeneoUsername',
        'akeneoPassword',
        'CommerceCloudURL',
        'commerceUsername',
        'commercePassword'
    ].each { p ->
        if (msg.getProperty(p) == null) {
            msg.setProperty(p, 'placeholder')
            log?.addAttachmentAsString("Property-Default-$p", 'placeholder', "text/plain")
        }
    }
}

// === 2. Akeneo-Authorisierung =====================================================
/**
 * Fordert über GraphQL ein Access-Token an und setzt es als Header "X-PIM-TOKEN".
 */
def callAkeneoAuthorize(Message msg, def log) {
    String gqlUrl       = msg.getProperty('AKN GraphQl URL')
    String clientId     = msg.getHeader('X-PIM-CLIENT-ID', String)
    String clientSecret = msg.getHeader('aknPIMClientSecret', String)
    String pimUrl       = msg.getHeader('X-PIM-URL', String)

    String username     = msg.getProperty('akeneoUsername')
    String password     = msg.getProperty('akeneoPassword')

    String gqlBody = """
        {"query":"query getToken{ token(username: \\"${username}\\", password: \\"${password}\\", clientId: \\"${clientId}\\", clientSecret: \\"${clientSecret}\\"){ data { accessToken } } }"}
    """.trim()

    Map<String,String> headers = [
        'X-PIM-URL'      : pimUrl,
        'X-PIM-CLIENT-ID': clientId,
        'Content-Type'   : 'application/json'
    ]

    def (int status, String response) = doPostRequest(gqlUrl, gqlBody, headers, basicAuth(username, password))

    if (status != 200) {
        throw new RuntimeException("Akeneo Authorize Call fehlgeschlagen – HTTP $status")
    }

    def token = new JsonSlurper().parseText(response)?.data?.token?.data?.accessToken
    if (!token) {
        throw new RuntimeException("Akeneo Authorize Response enthält kein accessToken")
    }
    msg.setHeader('X-PIM-TOKEN', token)
    log?.addAttachmentAsString("Akeneo-Token", token, "text/plain")
}

// === 3. Kategorien abrufen ========================================================
/**
 * Ruft die Kategorien anhand des Headers "AKN PIM Category code" ab
 * und legt die JSON-Response als Body ab.
 */
def callAkeneoGetCategories(Message msg, def log) {
    String gqlUrl   = msg.getProperty('AKN GraphQl URL')
    String pimUrl   = msg.getHeader('X-PIM-URL', String)
    String clientId = msg.getHeader('X-PIM-CLIENT-ID', String)
    String token    = msg.getHeader('X-PIM-TOKEN', String)

    String catCode  = msg.getHeader('AKN PIM Category code', String)

    String gqlBody = """
        {"query":"query GetCategories { categories(codes: \\"${catCode}\\") { items { code } } }"}
    """.trim()

    Map<String,String> headers = [
        'X-PIM-URL'      : pimUrl,
        'X-PIM-CLIENT-ID': clientId,
        'X-PIM-TOKEN'    : token,
        'Content-Type'   : 'application/json'
    ]

    def (int status, String response) = doPostRequest(gqlUrl, gqlBody, headers, null)

    if (status != 200) {
        throw new RuntimeException("Akeneo GetCategories Call fehlgeschlagen – HTTP $status")
    }

    def json = new JsonSlurper().parseText(response)
    if (!json?.data?.categories?.items) {
        throw new RuntimeException("Akeneo Response enthält keine Kategorien")
    }

    msg.setBody(response)
    log?.addAttachmentAsString("Akeneo-Kategorien-JSON", response, "application/json")
}

// === 4. Mapping JSON → XML ========================================================
/**
 * Wandelt die Akeneo-Kategorie-Liste in das Commerce-XML-Format um.
 */
def mapCategoriesToCommerceXml(Message msg, def log) {
    def json = new JsonSlurper().parseText(msg.getBody(String))

    String catId      = msg.getProperty('catId')
    String catVersion = msg.getProperty('catVersion')

    def writer = new StringWriter()
    def xml    = new MarkupBuilder(writer)

    xml.Categories {
        json.data.categories.items.each { item ->
            Category {
                catalogVersion {
                    CatalogVersion {
                        catalog {
                            Catalog {
                                id(catId)
                                integrationKey(catId)
                            }
                        }
                        version(catVersion)
                        integrationKey(catVersion)
                    }
                }
                code(item.code)
                integrationKey(item.code)
            }
        }
    }

    String xmlOut = writer.toString()
    msg.setBody(xmlOut)
    msg.setHeader('Content-Type', 'application/xml')
    log?.addAttachmentAsString("Commerce-XML", xmlOut, "application/xml")
}

// === 5. Senden an Commerce Cloud ===================================================
/**
 * Schickt den XML-Body an die Commerce Cloud. Die HTTP-Response wird
 * als Attachment gespeichert.
 */
def callSendToCommerce(Message msg, def log) {
    String url  = msg.getProperty('CommerceCloudURL')
    String user = msg.getProperty('commerceUsername')
    String pwd  = msg.getProperty('commercePassword')

    Map<String,String> headers = ['Content-Type':'application/xml']
    def (int status, String response) = doPostRequest(url, msg.getBody(String), headers, basicAuth(user, pwd))

    log?.addAttachmentAsString("Commerce-Response", "HTTP $status\n$response", "text/plain")

    if (status >= 400) {
        throw new RuntimeException("Commerce Cloud Call fehlgeschlagen – HTTP $status")
    }
    // Erfolgs-Status kann bei Bedarf als Property gesetzt werden
    msg.setProperty('commerceResponseCode', status)
}

// === Hilfsfunktionen ==============================================================
/**
 * Führt einen HTTP-POST aus und liefert [Statuscode, Response-String] zurück.
 */
def doPostRequest(String urlStr, String body, Map<String,String> headers, String basicAuthHeader) {
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod('POST')
    conn.doOutput = true
    headers?.each { k,v -> conn.setRequestProperty(k, v) }
    if (basicAuthHeader) conn.setRequestProperty('Authorization', basicAuthHeader)

    conn.outputStream.withWriter("UTF-8") { it << body }
    int status = conn.responseCode
    String respText = (status >= 200 && status < 400 ? conn.inputStream : conn.errorStream)?.getText('UTF-8') ?: ''
    conn.disconnect()
    return [status, respText]
}

/**
 * Erzeugt den Basic-Auth-Header-Wert.
 */
def basicAuth(String user, String pwd) {
    return "Basic " + "${user ?: ''}:${pwd ?: ''}".bytes.encodeBase64().toString()
}

// === Zentrales Error-Handling =====================================================
/**
 * Hängt den fehlerhaften Payload als Attachment an und wirft eine RuntimeException.
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: '', "text/plain")
    def errorMsg = "Fehler im Kategorie-Integrationsskript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}