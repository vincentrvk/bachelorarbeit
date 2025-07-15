/*************************************************************************
 *  SAP Cloud Integration – Groovy-Skript                                 *
 *  Import von Kategorien (Akeneo  ➜  SAP Commerce Cloud)                 *
 *                                                                       *
 *  Autor:   ChatGPT – Senior Integration Developer                       *
 *************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.net.HttpURLConnection

/*************************************************************************
 *  Haupteinstieg                                                         *
 *************************************************************************/
Message processData(Message message) {

    /*---------------------------------------------------------------------
     *  Logging-Instanz holen
     *-------------------------------------------------------------------*/
    def messageLog = messageLogFactory.getMessageLog(message)

    try {

        /*-----------------------------------------------------------------
         *  1) Header / Property-Werte ermitteln
         *---------------------------------------------------------------*/
        def ctx = getContextValues(message, messageLog)

        /*-----------------------------------------------------------------
         *  2) Akeneo – Token besorgen
         *---------------------------------------------------------------*/
        def pimToken = callAkeneoAuthorize(ctx, messageLog)
        message.setHeader('X-PIM-TOKEN', pimToken)

        /*-----------------------------------------------------------------
         *  3) Kategorie-Daten aus Akeneo lesen
         *---------------------------------------------------------------*/
        def categories = callAkeneoGetData(ctx, messageLog)

        /*-----------------------------------------------------------------
         *  4) Mapping – JSON ➜ XML   (SAP Commerce Cloud Format)
         *---------------------------------------------------------------*/
        def xmlBody = buildCategoriesXml(categories,
                                         ctx.catId,
                                         ctx.catVersion)

        /*-----------------------------------------------------------------
         *  5) Kategorien an Commerce Cloud senden
         *---------------------------------------------------------------*/
        callCommerceCloud(xmlBody, ctx, messageLog)

        /*-----------------------------------------------------------------
         *  6) Body und Rückgabe
         *---------------------------------------------------------------*/
        message.setBody(xmlBody)
        return message

    } catch (Exception e) {
        /*---------------------------------------------------------------
         *  Globales Error-Handling
         *-------------------------------------------------------------*/
        handleError(message.getBody(String) as String, e, messageLog)
    }

    /* niemals erreicht – return in try/ catch                                    */
    return message
}

/*************************************************************************
 *  Funktion: Kontextwerte einsammeln                                     *
 *************************************************************************/
def getContextValues(Message message, def messageLog) {

    /* Hilfsclosure */
    def getVal = { def src, String key ->
        src?.containsKey(key) && src[key] != null ? src[key].toString() : 'placeholder'
    }

    def hdr = message.getHeaders()
    def prp = message.getProperties()

    /* Context-Map mit allen benötigten Werten                            */
    def ctx = [
            aknPIMClientSecret   : getVal(hdr, 'aknPIMClientSecret'),
            pimUrl               : getVal(hdr, 'X-PIM-URL'),
            pimClientId          : getVal(hdr, 'X-PIM-CLIENT-ID'),
            pimCategoryCode      : getVal(hdr, 'AKN PIM Category code'),

            catId                : getVal(prp, 'catId'),
            catVersion           : getVal(prp, 'catVersion'),
            pimGraphQlUrl        : getVal(prp, 'AKN GraphQl URL'),
            akeneoUsername       : getVal(prp, 'akeneoUsername'),
            akeneoPassword       : getVal(prp, 'akeneoPassword'),

            commerceUrl          : getVal(prp, 'CommerceCloudURL'),
            commerceUsername     : getVal(prp, 'commerceUsername'),
            commercePassword     : getVal(prp, 'commercePassword')
    ]

    /* Fehlende Werte als Header / Property nachziehen (optional)         */
    ctx.each { k, v ->
        if (v == 'placeholder') {
            // Nur informativ loggen – keine Pflicht
            messageLog?.addAttachmentAsString("Missing_${k}", v, 'text/plain')
        }
    }

    return ctx
}

/*************************************************************************
 *  Funktion: Akeneo – Token holen                                        *
 *************************************************************************/
def String callAkeneoAuthorize(def ctx, def messageLog) {

    def url = new URL(ctx.pimGraphQlUrl)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.with {
        requestMethod = 'POST'
        doOutput      = true
        setRequestProperty('Content-Type', 'application/json')
        setRequestProperty('X-PIM-URL', ctx.pimUrl)
        setRequestProperty('X-PIM-CLIENT-ID', ctx.pimClientId)

        /* Basic-Auth */
        def auth = "${ctx.akeneoUsername}:${ctx.akeneoPassword}"
        def encAuth = Base64.encoder.encodeToString(auth.getBytes(StandardCharsets.UTF_8))
        setRequestProperty('Authorization', "Basic ${encAuth}")

        /* Body schreiben */
        def body = [
                query:
                        """query getToken{
                             token(
                               username: "${ctx.akeneoUsername}",
                               password: "${ctx.akeneoPassword}",
                               clientId: "${ctx.pimClientId}",
                               clientSecret: "${ctx.aknPIMClientSecret}"
                             ){
                               data{ accessToken }
                             }
                           }"""
        ]
        outputStream.withWriter('UTF-8') { it << groovy.json.JsonOutput.toJson(body) }
    }

    /* Response auswerten                                                 */
    if (conn.responseCode != 200) {
        throw new RuntimeException("Akeneo Authorize Call fehlgeschlagen – HTTP ${conn.responseCode}")
    }

    def json = new JsonSlurper().parse(conn.inputStream)
    def token = json?.data?.token?.data?.accessToken
    if (!token) {
        throw new RuntimeException('Akeneo Authorize Call – accessToken nicht gefunden')
    }

    messageLog?.addAttachmentAsString('AkeneoToken', token, 'text/plain')
    return token
}

/*************************************************************************
 *  Funktion: Akeneo – Kategorien laden                                   *
 *************************************************************************/
def List<String> callAkeneoGetData(def ctx, def messageLog) {

    def url = new URL(ctx.pimGraphQlUrl)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.with {
        requestMethod = 'POST'
        doOutput      = true
        setRequestProperty('Content-Type', 'application/json')
        setRequestProperty('X-PIM-URL', ctx.pimUrl)
        setRequestProperty('X-PIM-CLIENT-ID', ctx.pimClientId)
        setRequestProperty('X-PIM-TOKEN', messageLog.getMessage().getHeader('X-PIM-TOKEN', String))

        /* Basic-Auth */
        def auth = "${ctx.akeneoUsername}:${ctx.akeneoPassword}"
        def encAuth = Base64.encoder.encodeToString(auth.getBytes(StandardCharsets.UTF_8))
        setRequestProperty('Authorization', "Basic ${encAuth}")

        /* Body schreiben */
        def body = [
                query:
                        """query GetCategories{
                             categories(codes: "${ctx.pimCategoryCode}"){
                               items{ code }
                             }
                           }"""
        ]
        outputStream.withWriter('UTF-8') { it << groovy.json.JsonOutput.toJson(body) }
    }

    /* Response prüfen                                                    */
    if (conn.responseCode != 200) {
        throw new RuntimeException("Akeneo GetData Call fehlgeschlagen – HTTP ${conn.responseCode}")
    }

    def json = new JsonSlurper().parse(conn.inputStream)
    def items = json?.data?.categories?.items
    if (!items || items.isEmpty()) {
        throw new RuntimeException('Akeneo GetData Call – keine Kategorien erhalten')
    }

    /* Liste mit Category Codes zurückgeben                               */
    def codes = items.collect { it.code.toString() }
    messageLog?.addAttachmentAsString('AkeneoCategories', codes.toString(), 'text/plain')
    return codes
}

/*************************************************************************
 *  Funktion: Mapping JSON ➜ XML                                          *
 *************************************************************************/
def String buildCategoriesXml(List<String> codes, String catId, String catVersion) {

    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)

    xml.Categories {
        codes.each { String c ->
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
    return writer.toString()
}

/*************************************************************************
 *  Funktion: Kategorien an Commerce Cloud senden                         *
 *************************************************************************/
def void callCommerceCloud(String body, def ctx, def messageLog) {

    def url = new URL(ctx.commerceUrl)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.with {
        requestMethod = 'POST'
        doOutput      = true
        setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')

        /* Basic-Auth */
        def auth = "${ctx.commerceUsername}:${ctx.commercePassword}"
        def encAuth = Base64.encoder.encodeToString(auth.getBytes(StandardCharsets.UTF_8))
        setRequestProperty('Authorization', "Basic ${encAuth}")

        /* Body schreiben */
        outputStream.withWriter('UTF-8') { it << body }
    }

    /* Ergebnis loggen / Fehler werfen                                   */
    if (conn.responseCode >= 200 && conn.responseCode < 300) {
        messageLog?.addAttachmentAsString('CommerceCloudResponse',
                                           conn.inputStream?.text ?: '',
                                           'text/plain')
    } else {
        throw new RuntimeException("Commerce Cloud Call fehlgeschlagen – HTTP ${conn.responseCode}")
    }
}

/*************************************************************************
 *  Funktion: Zentrales Error-Handling                                     *
 *************************************************************************/
def handleError(String body, Exception e, def messageLog) {
    /* Payload als Attachment sichern                                     */
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    /* Fehler werfen, damit CPI die Exception erkennt                     */
    throw new RuntimeException("Fehler im Kategorien-Import: ${e.message}", e)
}