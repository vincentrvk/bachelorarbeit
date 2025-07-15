/*****************************************************************************************
 *  Akeneo -> SAP Commerce Cloud  –  Category Import Script
 *
 *  Autor  : CPI-Groovy-Integration-Team
 *  Stand  : 2025-06-11
 *
 *  Beschreibung:
 *  Das Skript
 *    1. liest benötigte Header & Properties ein (bzw. setzt „placeholder“).
 *    2. holt ein Access-Token aus Akeneo (GraphQL Authorize).
 *    3. liest per GraphQL die angeforderten Kategorien.
 *    4. mappt das Ergebnis auf das gewünschte Commerce-Cloud-XML-Format.
 *    5. sendet das XML an die Commerce Cloud.
 *
 *  Alle Funktionen sind modular aufgebaut, vollständig deutsch kommentiert
 *  und mit aussagekräftigem Error-Handling versehen.
 *****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets

Message processData(Message message) {

    /*--------------------------------------------------------------*
     * Lokale Funktionsdefinitionen
     *--------------------------------------------------------------*/

    /**
     * Stellt sicher, dass alle geforderten Header & Properties
     * vorhanden sind. Fehlende Werte werden mit „placeholder“
     * initialisiert.
     */
    def ensureHeadersAndProperties = { Message msg, def log ->
        // Header-Liste
        ['aknPIMClientSecret',
         'X-PIM-URL',
         'X-PIM-CLIENT-ID',
         'Content-Type',
         'AKN PIM Category code'
        ].each { h ->
            if (msg.getHeader(h, String.class) == null) {
                msg.setHeader(h, h == 'Content-Type' ? 'application/json' : 'placeholder')
                log?.addAttachmentAsString("AutoHeader_$h",
                        "Header $h nicht vorhanden – setze placeholder", 'text/plain')
            }
        }

        // Property-Liste
        ['catId',
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
                log?.addAttachmentAsString("AutoProperty_$p",
                        "Property $p nicht vorhanden – setze placeholder", 'text/plain')
            }
        }
    }

    /**
     * Führt den GraphQL-Authorize-Call aus und schreibt das Access-Token
     * in den Header „X-PIM-TOKEN“.
     */
    def callAkeneoAuthorize = { Message msg, def log ->
        try {
            String gqlUrl    = msg.getProperty('AKN GraphQl URL') as String
            String clientId  = msg.getHeader ('X-PIM-CLIENT-ID')   as String
            String secret    = msg.getHeader ('aknPIMClientSecret') as String
            String user      = msg.getProperty('akeneoUsername')   as String
            String pass      = msg.getProperty('akeneoPassword')   as String
            String xPimUrl   = msg.getHeader ('X-PIM-URL')         as String

            // GraphQL-Query zusammenbauen
            String query = """query getToken{
                                token(
                                  username    : "$user",
                                  password    : "$pass",
                                  clientId    : "$clientId",
                                  clientSecret: "$secret"){
                                    data{ accessToken }
                                  }
                              }""".replaceAll('\\s+', ' ')  // unnötige Leerzeichen entfernen

            String payload = JsonOutput.toJson([query: query])

            // HTTP-Aufruf
            HttpURLConnection con =
                    (HttpURLConnection) new URL(gqlUrl).openConnection()
            con.with {
                requestMethod      = 'POST'
                doOutput           = true
                setRequestProperty 'Content-Type',       'application/json'
                setRequestProperty 'X-PIM-URL',          xPimUrl
                setRequestProperty 'X-PIM-CLIENT-ID',    clientId
                // BasicAuth clientId:secret
                String basicAuth   = "$clientId:$secret".bytes.encodeBase64().toString()
                setRequestProperty 'Authorization', "Basic $basicAuth"
                outputStream.withCloseable { it.write(payload.getBytes(StandardCharsets.UTF_8)) }
            }

            int rc = con.responseCode
            String respBody = con.inputStream.getText('UTF-8')
            log?.addAttachmentAsString('AuthorizeResponse', respBody, 'application/json')

            if (rc != 200) throw new RuntimeException("HTTP-RC $rc beim Token-Abruf")

            def json = new JsonSlurper().parseText(respBody)
            String token = json?.data?.token?.data?.accessToken
            if (!token) throw new RuntimeException('Kein Access-Token in Response gefunden')

            msg.setHeader('X-PIM-TOKEN', token)
        } catch (Exception e) {
            handleError(msg.getBody(String) as String, e, log)
        }
    }

    /**
     * Ruft Akeneo ab und liefert eine Liste der Kategorien (Codes) zurück.
     */
    def callAkeneoGetData = { Message msg, def log ->
        try {
            String gqlUrl    = msg.getProperty('AKN GraphQl URL')  as String
            String clientId  = msg.getHeader ('X-PIM-CLIENT-ID')   as String
            String token     = msg.getHeader ('X-PIM-TOKEN')       as String
            String xPimUrl   = msg.getHeader ('X-PIM-URL')         as String
            String catCode   = msg.getHeader ('AKN PIM Category code') as String

            String query = """query GetCategories{
                                categories(codes: "$catCode"){
                                  items { code }
                                }
                              }""".replaceAll('\\s+', ' ')
            String payload = JsonOutput.toJson([query: query])

            HttpURLConnection con =
                    (HttpURLConnection) new URL(gqlUrl).openConnection()
            con.with {
                requestMethod      = 'POST'
                doOutput           = true
                setRequestProperty 'Content-Type',    'application/json'
                setRequestProperty 'X-PIM-CLIENT-ID', clientId
                setRequestProperty 'X-PIM-TOKEN',     token
                setRequestProperty 'X-PIM-URL',       xPimUrl
                // BasicAuth wie bei Authorize
                String basicAuth   = "$clientId:${msg.getHeader('aknPIMClientSecret')}"
                                        .bytes.encodeBase64().toString()
                setRequestProperty 'Authorization', "Basic $basicAuth"
                outputStream.withCloseable { it.write(payload.getBytes(StandardCharsets.UTF_8)) }
            }

            int rc = con.responseCode
            String respBody = con.inputStream.getText('UTF-8')
            log?.addAttachmentAsString('GetDataResponse', respBody, 'application/json')

            if (rc != 200) throw new RuntimeException("HTTP-RC $rc beim Kategorien-Abruf")

            def json = new JsonSlurper().parseText(respBody)
            def items = json?.data?.categories?.items
            if (!items || items.isEmpty())
                throw new RuntimeException('Keine Kategorien im Response gefunden')

            return items.collect { it.code as String }
        } catch (Exception e) {
            handleError(msg.getBody(String) as String, e, log)
            return [] // compiler-beruhigung
        }
    }

    /**
     * Erstellt das Zielschema-XML anhand der Kategorien-Liste.
     */
    def performMapping = { Message msg, List<String> codes, def log ->
        try {
            StringWriter sw = new StringWriter()
            MarkupBuilder xml = new MarkupBuilder(sw)
            String catId      = msg.getProperty('catId')      as String
            String catVersion = msg.getProperty('catVersion') as String

            xml.Categories {
                codes.each { c ->
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
                        code(c)
                        integrationKey(c)
                    }
                }
            }
            return sw.toString()
        } catch (Exception e) {
            handleError(msg.getBody(String) as String, e, log)
            return ''
        }
    }

    /**
     * Sendet das erstellte XML an die Commerce Cloud.
     */
    def sendToCommerceCloud = { Message msg, String xmlBody, def log ->
        try {
            String urlStr   = msg.getProperty('CommerceCloudURL') as String
            String user     = msg.getProperty('commerceUsername') as String
            String pass     = msg.getProperty('commercePassword') as String
            String bAuth    = "$user:$pass".bytes.encodeBase64().toString()

            HttpURLConnection con =
                    (HttpURLConnection) new URL(urlStr).openConnection()
            con.with {
                requestMethod      = 'POST'
                doOutput           = true
                setRequestProperty 'Content-Type',  'application/xml'
                setRequestProperty 'Authorization', "Basic $bAuth"
                outputStream.withCloseable { it.write(xmlBody.getBytes(StandardCharsets.UTF_8)) }
            }

            int rc = con.responseCode
            String resp = (rc >=200 && rc <300) ?
                    con.inputStream.getText('UTF-8') :
                    con.errorStream?.getText('UTF-8')
            log?.addAttachmentAsString('CommerceCloudResponse', resp ?: '', 'text/plain')

            if (rc < 200 || rc >= 300)
                throw new RuntimeException("Fehler bei Commerce-Cloud-Call (RC=$rc)")
        } catch (Exception e) {
            handleError(xmlBody, e, log)
        }
    }

    /**
     * Einheitliches Error-Handling.
     * Fügt den fehlerhaften Payload als Attachment hinzu und wirft RuntimeException.
     */
    def handleError = { String body, Exception e, def log ->
        log?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
        def err = "Fehler im Akeneo-Import-Skript: ${e.message}"
        throw new RuntimeException(err, e)
    }

    /*--------------------------------------------------------------*
     * Hauptverarbeitung
     *--------------------------------------------------------------*/
    def messageLog = messageLogFactory?.getMessageLog(message)

    try {
        ensureHeadersAndProperties(message, messageLog)
        callAkeneoAuthorize(message, messageLog)
        List<String> codes = callAkeneoGetData(message, messageLog)
        String xmlBody    = performMapping(message, codes, messageLog)

        // Message-Body für potentielle weitere CPI-Schritte aktualisieren
        message.setBody(xmlBody)
        // Direkt an Commerce Cloud senden
        sendToCommerceCloud(message, xmlBody, messageLog)

    } catch (Exception ex) {
        // Gesamt-Catch als Fallback
        handleError(message.getBody(String) as String, ex, messageLog)
    }

    return message
}