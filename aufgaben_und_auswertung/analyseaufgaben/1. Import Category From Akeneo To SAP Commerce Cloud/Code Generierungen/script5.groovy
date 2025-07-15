/*****************************************************************************************
 *  Groovy-Skript: Akeneo Categories → SAP Commerce Cloud
 *  Beschreibung  : Ruft Kategorien aus Akeneo per GraphQL ab, mappt sie in das
 *                  Commerce-XML-Format und sendet die Daten an die SAP Commerce Cloud.
 *  Autor         : AI-Assistant
 *****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection

/* =========================================================================
 *  Einstiegspunkt
 * ========================================================================= */
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1. Konfiguration aus Headers & Properties holen                    */
        def cfg = gatherConfiguration(message, messageLog)

        /* 2. Akeneo Autorisierung (Token holen)                              */
        cfg.X_PIM_TOKEN = callAkeneoAuthorize(cfg, message, messageLog)

        /* 3. Kategorien von Akeneo holen                                     */
        def categoryCodes = getCategoriesData(cfg, messageLog)

        if (!categoryCodes || categoryCodes.isEmpty()) {
            throw new RuntimeException('Akeneo Response enthält keine Kategorien.')
        }

        /* 4. Mapping JSON → XML                                              */
        def xmlPayload = mapToCommerceXML(categoryCodes, cfg, messageLog)

        /* 5. Kategorien an Commerce Cloud senden                             */
        int ccResponse = sendToCommerceCloud(xmlPayload, cfg, messageLog)

        /* 6. Body & Rückgabe setzen                                          */
        message.setBody(xmlPayload)
        message.setHeader('CommerceCloudResponseCode', ccResponse)

        return message

    } catch (Exception e) {
        handleError(message.getBody(String) as String, e, messageLog)
    }
}

/* =========================================================================
 *  Funktion: gatherConfiguration
 * -------------------------------------------------------------------------
 *  Liest benötigte Konfigurationswerte aus Message-Headers und -Properties
 *  oder ersetzt sie durch „placeholder“, falls nicht vorhanden.
 * ========================================================================= */
def gatherConfiguration(Message msg, def msgLog) {

    def getHeader = { name -> (msg.getHeader(name, String) ?: 'placeholder') }
    def getProp   = { name -> (msg.getProperty(name) ?: 'placeholder')      }

    def cfg = [
        /* Header                                                  */
        pimUrl        : getHeader('X-PIM-URL'),
        clientId      : getHeader('X-PIM-CLIENT-ID'),
        clientSecret  : getHeader('aknPIMClientSecret'),
        categoryCode  : getHeader('AKN PIM Category code'),

        /* Properties                                              */
        graphQlUrl        : getProp('AKN GraphQl URL'),
        akeneoUsername    : getProp('akeneoUsername'),
        akeneoPassword    : getProp('akeneoPassword'),
        catId             : getProp('catId'),
        catVersion        : getProp('catVersion'),
        commerceUrl       : getProp('CommerceCloudURL'),
        commerceUsername  : getProp('commerceUsername'),
        commercePassword  : getProp('commercePassword')
    ]

    /* Für Debug-Zwecke Konfiguration loggen (ohne Passwörter)     */
    msgLog?.addAttachmentAsString('Configuration', cfg.findAll {
        !it.key.toLowerCase().contains('password')
    }.toString(), 'text/plain')

    return cfg
}

/* =========================================================================
 *  Funktion: callAkeneoAuthorize
 * -------------------------------------------------------------------------
 *  Ruft das Akeneo-GraphQL-Endpoint auf und liefert ein Access-Token zurück.
 * ========================================================================= */
String callAkeneoAuthorize(Map cfg, Message msg, def msgLog) {

    HttpURLConnection conn
    try {
        conn = (HttpURLConnection) new URL(cfg.graphQlUrl).openConnection()
        conn.with {
            requestMethod  = 'POST'
            doOutput       = true
            setRequestProperty('Content-Type',        'application/json')
            setRequestProperty('X-PIM-URL',           cfg.pimUrl)
            setRequestProperty('X-PIM-CLIENT-ID',     cfg.clientId)
            setRequestProperty('Authorization',       basicAuth(cfg.akeneoUsername, cfg.akeneoPassword))
        }

        /* GraphQL-Body erstellen                                          */
        def gqlBody = [
            query: """query getToken{
                         token(username: "${cfg.akeneoUsername}",
                               password: "${cfg.akeneoPassword}",
                               clientId: "${cfg.clientId}",
                               clientSecret: "${cfg.clientSecret}") {
                           data { accessToken }
                         }
                       }"""
        ]
        conn.outputStream.withWriter('UTF-8') { it << new groovy.json.JsonBuilder(gqlBody).toString() }

        int rc = conn.responseCode
        def responseTxt = conn.inputStream.getText('UTF-8')

        msgLog?.addAttachmentAsString('AkeneoAuthResponse', responseTxt, 'application/json')

        if (rc != 200) {
            throw new RuntimeException("Akeneo Authorize Call fehlgeschlagen (HTTP $rc)")
        }

        def token = new JsonSlurper().parseText(responseTxt)?.data?.token?.data?.accessToken
        if (!token) {
            throw new RuntimeException('Token konnte nicht aus Akeneo-Antwort ermittelt werden.')
        }

        /* Header für Folgeschritte ablegen                               */
        msg.setHeader('X-PIM-TOKEN', token)
        return token

    } finally {
        conn?.disconnect()
    }
}

/* =========================================================================
 *  Funktion: getCategoriesData
 * -------------------------------------------------------------------------
 *  Ruft die Kategorien anhand des übergebenen Codes aus Akeneo ab und
 *  liefert eine Liste der Kategorie-Codes zurück.
 * ========================================================================= */
List<String> getCategoriesData(Map cfg, def msgLog) {

    HttpURLConnection conn
    try {
        conn = (HttpURLConnection) new URL(cfg.graphQlUrl).openConnection()
        conn.with {
            requestMethod  = 'POST'
            doOutput       = true
            setRequestProperty('Content-Type',    'application/json')
            setRequestProperty('X-PIM-URL',       cfg.pimUrl)
            setRequestProperty('X-PIM-CLIENT-ID', cfg.clientId)
            setRequestProperty('X-PIM-TOKEN',     cfg.X_PIM_TOKEN)
            setRequestProperty('Authorization',   basicAuth(cfg.akeneoUsername, cfg.akeneoPassword))
        }

        def gqlBody = [
            query: """query GetCategories{
                         categories(codes: "${cfg.categoryCode}") {
                             items { code }
                         }
                     }"""
        ]
        conn.outputStream.withWriter('UTF-8') { it << new groovy.json.JsonBuilder(gqlBody).toString() }

        int rc = conn.responseCode
        def responseTxt = conn.inputStream.getText('UTF-8')
        msgLog?.addAttachmentAsString('AkeneoCategoryResponse', responseTxt, 'application/json')

        if (rc != 200) {
            throw new RuntimeException("Akeneo GetCategories Call fehlgeschlagen (HTTP $rc)")
        }

        def json = new JsonSlurper().parseText(responseTxt)
        def items = json?.data?.categories?.items
        return items?.collect { it.code } ?: []

    } finally {
        conn?.disconnect()
    }
}

/* =========================================================================
 *  Funktion: mapToCommerceXML
 * -------------------------------------------------------------------------
 *  Wandelt die übergebenen Kategorie-Codes in das gewünschte XML-Format um.
 * ========================================================================= */
String mapToCommerceXML(List<String> codes, Map cfg, def msgLog) {

    def sw = new StringWriter()
    def xml = new MarkupBuilder(sw)
    xml.setDoubleQuotes(true)   // besseres Escaping

    xml.Categories {
        codes.each { catCode ->
            Category {
                catalogVersion {
                    CatalogVersion {
                        catalog {
                            Catalog {
                                id(cfg.catId)
                                integrationKey(cfg.catId)
                            }
                        }
                        version(cfg.catVersion)
                        integrationKey(cfg.catVersion)
                    }
                }
                code(catCode)
                integrationKey(catCode)
            }
        }
    }

    def xmlOut = '<?xml version="1.0" encoding="UTF-8"?>' + sw.toString()
    msgLog?.addAttachmentAsString('MappedXML', xmlOut, 'application/xml')
    return xmlOut
}

/* =========================================================================
 *  Funktion: sendToCommerceCloud
 * -------------------------------------------------------------------------
 *  Sendet das XML per POST an die Commerce Cloud.
 * ========================================================================= */
int sendToCommerceCloud(String xmlBody, Map cfg, def msgLog) {

    HttpURLConnection conn
    try {
        conn = (HttpURLConnection) new URL(cfg.commerceUrl).openConnection()
        conn.with {
            requestMethod  = 'POST'
            doOutput       = true
            setRequestProperty('Content-Type', 'application/xml')
            setRequestProperty('Authorization', basicAuth(cfg.commerceUsername, cfg.commercePassword))
        }

        conn.outputStream.withWriter('UTF-8') { it << xmlBody }
        int rc = conn.responseCode
        def resp = (rc >= 200 && rc < 300) ? conn.inputStream : conn.errorStream
        def respTxt = resp?.getText('UTF-8') ?: ''
        msgLog?.addAttachmentAsString('CommerceCloudResponse', respTxt, 'text/plain')

        if (rc >= 300) {
            throw new RuntimeException("Commerce Cloud Call fehlgeschlagen (HTTP $rc)")
        }
        return rc

    } finally {
        conn?.disconnect()
    }
}

/* =========================================================================
 *  Hilfsfunktion: basicAuth
 * -------------------------------------------------------------------------
 *  Baut den Basic-Auth-Header für Benutzer & Passwort.
 * ========================================================================= */
String basicAuth(String user, String pw) {
    return 'Basic ' + "${user ?: ''}:${pw ?: ''}".bytes.encodeBase64().toString()
}

/* =========================================================================
 *  Error-Handling
 * -------------------------------------------------------------------------
 *  Hängt das Eingangspayload an die CPI-Monitor-Nachricht an und wirft
 *  eine RuntimeException mit klarer Meldung.
 * ========================================================================= */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    def errorMsg = "Fehler im Kategorie-Import-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}