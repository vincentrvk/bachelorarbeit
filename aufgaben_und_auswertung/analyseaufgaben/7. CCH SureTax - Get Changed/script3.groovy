/****************************************************************************************
*  Groovy-Skript:   S/4HANA Cloud  ➜  CCH SureTax  – Changed Jurisdictions abrufen
*  Autor:           Senior-Integration-Consultant
*  Beschreibung:    – Request-Mapping zu SureTax
*                  – HTTP-Aufruf „GetAllGeocodes“
*                  – Response-Mapping zurück nach S/4
*                  – Logging (als Attachments)
*                  – zentrales Error-Handling
*****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import groovy.xml.XmlSlurper
import java.io.StringWriter
import java.net.HttpURLConnection
import java.net.URL

/* ================================================================
 *  Einstiegspunkt
 * ============================================================== */
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* ---------- 0. Eingehenden Payload loggen ---------------- */
        addLogAttachment(messageLog, '01-IncomingPayload', message.getBody(String) ?: '')

        /* ---------- 1. Kontextwerte sammeln & validieren ---------- */
        def ctx = collectContextValues(message)

        /* ---------- 2. Request-Mapping erstellen ----------------- */
        String requestXml = buildRequestPayload(ctx.username, ctx.password)
        addLogAttachment(messageLog, '02-RequestPayload', requestXml)

        /* ---------- 3. API-Aufruf „Get Changed“ ------------------ */
        String responseRaw = callGetChangedApi(requestXml, ctx.url, ctx.username, ctx.password)
        addLogAttachment(messageLog, '03-ResponseRaw', responseRaw)

        /* ---------- 4. Response-Mapping zurück nach S/4 ---------- */
        String mappedResponse = mapResponse(responseRaw, ctx.unifyInd, ctx.stateMap)
        addLogAttachment(messageLog, '04-MappedResponse', mappedResponse)

        /* ---------- 5. Ergebnis in Message-Body schreiben -------- */
        message.setBody(mappedResponse)
        return message

    } catch (Exception e) {
        /* Einheitliches Error-Handling */
        handleError(message.getBody(String) ?: '', e, messageLog)
    }
}

/* ================================================================
 *  Kontextwerte (Header & Properties) ermitteln
 * ============================================================== */
def collectContextValues(Message message) {
    String username     = message.getProperty('sureTaxUsername')     ?: 'placeholder'
    String password     = message.getProperty('sureTaxPassword')     ?: 'placeholder'
    String url          = message.getProperty('sureTaxURL')          ?: 'placeholder'
    String unifyInd     = message.getProperty('exchangejcdunifyind') ?: null
    Map   stateMapTmp   = message.getProperty('stateMap') instanceof Map
                        ? message.getProperty('stateMap')
                        : [:]

    /* Pflicht-Property prüfen */
    if (!unifyInd) {
        throw new RuntimeException("Pflicht-Property 'exchangejcdunifyind' nicht vorhanden oder leer.")
    }

    [
        username : username,
        password : password,
        url      : url,
        unifyInd : unifyInd.toString(),
        stateMap : stateMapTmp
    ]
}

/* ================================================================
 *  Request-Mapping erzeugen
 * ============================================================== */
String buildRequestPayload(String username, String password) {
    StringWriter writer = new StringWriter()
    new MarkupBuilder(writer).'ns3:GetAllGeocodes'('xmlns:ns3': 'http://soa.noventic.com/GeocodeService/GeocodeService-V1') {
        'ns3:ClientNumber'(username)
        'ns3:ValidationKey'(password)
    }
    writer.toString()
}

/* ================================================================
 *  HTTP-Call zu SureTax ausführen
 * ============================================================== */
String callGetChangedApi(String body, String url, String username, String password) {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection()
    connection.with {
        requestMethod = 'POST'
        doOutput      = true
        setRequestProperty('Authorization', 'Basic ' + "${username}:${password}".bytes.encodeBase64().toString())
        setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')
        outputStream.withWriter('UTF-8') { it << body }
    }

    int status = connection.responseCode
    String responseText = (status >= 200 && status < 300)
            ? connection.inputStream.getText('UTF-8')
            : connection.errorStream?.getText('UTF-8')

    if (status < 200 || status >= 300) {
        throw new RuntimeException("HTTP-Fehler ${status} beim Aufruf der SureTax-API. Response: ${responseText}")
    }
    responseText
}

/* ================================================================
 *  Response-Mapping durchführen
 * ============================================================== */
String mapResponse(String responseBody, String unifyInd, Map stateMap) {
    def slurper  = new XmlSlurper()
    def geocodes = slurper.parseText(responseBody)
                          .'**'.findAll { it.name() == 'string' }
                          .collect { it.text().trim() }
                          .findAll { it }                 // leere Strings entfernen
                          .collect { transformString(it, unifyInd, stateMap) }

    /* Ziel-XML erzeugen */
    StringWriter writer = new StringWriter()
    new MarkupBuilder(writer).'ns2:TAX_JUR_GETCHANGELIST_RECEIVE'('xmlns:ns2':'http://sap.com/xi/FotETaxUS') {
        'ns2:TAX_JURI_CODE' {
            geocodes.each { code ->
                'ns2:TXJCD'(code)
            }
        }
    }
    writer.toString()
}

/* ================================================================
 *  Regelbasierte Transformation eines einzelnen Geocodes
 * ============================================================== */
String transformString(String input, String unifyInd, Map stateMap) {
    if (unifyInd == 'X') {
        if (input.startsWith('ZZ')) {
            /* Regel 1: ZZ ➜ US */
            return 'US' + input.substring(2)
        } else if (input.startsWith('US') && input.length() >= 4) {
            /* Regel 2: US + State-Mapping + '-' */
            String stateKey   = input.substring(2, 4)
            String mapped     = stateMap.get(stateKey) ?: stateKey
            String rest       = input.substring(4)     // ab Position 5 (0-basiert)
            return 'US' + mapped + rest + '-'
        }
    }
    /* Regel 3 oder Default */
    input
}

/* ================================================================
 *  Attachment-Logging
 * ============================================================== */
void addLogAttachment(def messageLog, String name, String content) {
    messageLog?.addAttachmentAsString(name, content, 'text/xml')
}

/* ================================================================
 *  Zentrales Error-Handling
 * ============================================================== */
void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}