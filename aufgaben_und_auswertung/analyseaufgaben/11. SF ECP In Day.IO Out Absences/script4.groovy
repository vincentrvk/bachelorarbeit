/****************************************************************************************
 * Groovy-Skript für SAP Cloud Integration – Day.IO-Absences nach SAP ECC Payroll
 *
 * Autor:  ChatGPT (Senior-Integration Entwickler)
 * Version: 1.0
 *
 * Hinweis:
 *  - Das Skript setzt die in der Aufgabenbeschreibung genannten Properties/Header
 *    voraus bzw. erstellt sie mit dem Platzhalter „placeholder“.
 *  - Für jede Abwesenheit wird ein einzelner Aufruf an die Ziel-API ausgeführt, die
 *    Antworten werden gesammelt und als JSON zurückgeliefert.
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.*
import java.text.SimpleDateFormat
import groovy.xml.*

// ======================================================================================
// Entry-Point
// ======================================================================================
Message processData(Message message) {

    // 1. Kontext vorbereiten (Header & Properties)
    setupContext(message)

    // 2. Eingehenden Payload sichern (Attachment)
    def originalBody = message.getBody(String) ?: ''
    logPayload(message, 'OriginalPayload', originalBody)

    // 3. Eingehenden JSON request parsen
    def jsonSlurper = new JsonSlurper()
    Map root
    try {
        root = jsonSlurper.parseText(originalBody) as Map
    } catch (Exception e) {
        handleError(originalBody, e, messageLogFactory.getMessageLog(message))
    }

    List<Map> absences = (root?.request?.absences ?: []) as List<Map>
    if (!absences) {
        // Keine Abwesenheiten ==> nichts zu tun, aber eindeutige Meldung
        message.setBody(JsonOutput.toJson([response:[absences:[]]]))
        return message
    }

    // Liste für alle Response-Objekte
    List<Map> aggregatedResponseList = []

    // MessageLog für alle Schlüsseleinträge
    def msgLog = messageLogFactory.getMessageLog(message)

    // 4. Für jede Abwesenheit Request aufbauen, aufrufen & Response verarbeiten
    absences.each { Map singleAbsence ->

        // Abwesenheits-spezifische Properties setzen
        setAbsenceSpecificProperties(message, singleAbsence)

        // Request-Mapping
        String xmlRequest = buildAbsenceRequest(singleAbsence, message)
        logPayload(message, "Request_${singleAbsence.uniqueId ?: System.nanoTime()}", xmlRequest)

        // Aufruf der Fremd-API
        String rawResponse = performApiCall(xmlRequest, message)
        logPayload(message, "Response_${singleAbsence.uniqueId ?: System.nanoTime()}", rawResponse)

        // Response-Mapping
        Map mapped = mapApiResponse(rawResponse, message)
        aggregatedResponseList << mapped
    }

    // 5. Zusammengefasste Antwort erstellen
    Map responseWrapper = [response: [absences: (aggregatedResponseList.size() == 1 ?
                                                aggregatedResponseList[0] :
                                                aggregatedResponseList)]]

    String finalJson = JsonOutput.toJson(responseWrapper)
    message.setBody(finalJson)

    // Abschluss-Logging
    msgLog?.addAttachmentAsString('AggregatedResponse', finalJson, 'application/json')

    return message
}


// ======================================================================================
// Modul-Funktionen
// ======================================================================================

/**
 * Liest benötigte Header/Properties; falls nicht vorhanden => placeholder
 */
def setupContext(Message msg) {
    try {
        // Hilfsclosure zum Holen oder Setzen
        def ensure = { String key, boolean isHeader ->
            def val = isHeader ? msg.getHeader(key, String) : msg.getProperty(key)
            if (!val) {
                if (isHeader) {
                    msg.setHeader(key, 'placeholder')
                } else {
                    msg.setProperty(key, 'placeholder')
                }
            }
        }

        // Properties
        ['requestUser', 'requestPassword', 'requestURL',
         'p_operacao', 'p_externalId', 'p_startDate', 'p_uniqueId'].each { ensure(it, false) }

        // (In dieser Aufgabe keine expliziten Header erforderlich, ready for future use)
    } catch (Exception e) {
        handleError('ContextInitialisation', e, messageLogFactory.getMessageLog(msg))
    }
}

/**
 * Schreibt Abwesenheits-spezifische Properties ins Message-Objekt
 */
def setAbsenceSpecificProperties(Message msg, Map absence) {
    try {
        msg.setProperty('p_externalId', absence.externalId ?: 'placeholder')
        msg.setProperty('p_startDate',  absence.startDate ?: 'placeholder')
        msg.setProperty('p_uniqueId',   absence.uniqueId  ?: 'placeholder')
    } catch (Exception e) {
        handleError(absence?.toString(), e, messageLogFactory.getMessageLog(msg))
    }
}

/**
 * Baut das XML für eine einzelne Abwesenheit
 */
String buildAbsenceRequest(Map absence, Message msg) {
    try {
        def sw = new StringWriter()
        def xml = new MarkupBuilder(sw)
        def nsUri = 'urn:sap-com:document:sap:rfc:functions'

        // Datum- & Zeitformatierungen
        String begda = formatDate(absence.startDate)
        String beguz = formatTime(absence.startTime)
        String stdaz = absence.hours ?: ''

        xml.'ns1:ZHR_FGRP_0001'('xmlns:ns1': nsUri) {
            'ns1:P2001' {
                'ns1:item' {
                    'ns1:PERNR'(absence.externalId ?: '')
                    'ns1:BEGDA'(begda)
                    'ns1:BEGUZ'(beguz)
                    'ns1:STDAZ'(stdaz)
                }
            }
            'ns1:TP_OP'(determineTpOp(msg))
        }
        return sw.toString()
    } catch (Exception e) {
        handleError(absence?.toString(), e, messageLogFactory.getMessageLog(msg))
        return ''
    }
}

/**
 * Führt den HTTP-POST mittels BasicAuth aus
 */
String performApiCall(String xmlPayload, Message msg) {
    def log = messageLogFactory.getMessageLog(msg)
    try {
        String targetUrl = msg.getProperty('requestURL')
        URL url = new URL(targetUrl)
        HttpURLConnection conn = (HttpURLConnection) url.openConnection()
        conn.with {
            requestMethod = 'POST'
            doOutput      = true
            setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')

            String user = msg.getProperty('requestUser')
            String pwd  = msg.getProperty('requestPassword')
            String auth = "${user}:${pwd}".bytes.encodeBase64().toString()
            setRequestProperty('Authorization', "Basic ${auth}")

            // Payload senden
            outputStream.withWriter('UTF-8') { it << xmlPayload }
        }

        int rc = conn.responseCode
        String respBody = rc >= 200 && rc < 300 ?
                conn.inputStream.getText('UTF-8') :
                conn.errorStream?.getText('UTF-8') ?: ''

        if (rc < 200 || rc >= 300) {
            throw new RuntimeException("HTTP-Error (${rc}) beim Aufruf der Ziel-API.")
        }
        return respBody
    } catch (Exception e) {
        handleError(xmlPayload, e, log)
        return ''
    }
}

/**
 * Mapped das SAP-Response XML auf das gewünschte JSON-Format
 */
Map mapApiResponse(String responseXml, Message msg) {
    try {
        def slurper = new XmlSlurper(false, false)
        def root    = slurper.parseText(responseXml)

        // Wir nehmen nur das erste "item" aus RETURN, da jeweils Einzel-Aufruf
        def firstItem = root?.RETURN?.item?.getAt(0)

        String statusRaw = firstItem?.STATUS?.text() ?: ''
        String messageRaw = firstItem?.MENSAGEM?.text() ?: ''

        // OPTIONAL: Status-Übersetzung
        String statusMapped = statusRaw == 'S' ? 'SUCCESS' :
                              statusRaw == 'E' ? 'ERROR'   :
                              statusRaw

        return [
            externalId : msg.getProperty('p_externalId'),
            startDate  : msg.getProperty('p_startDate'),
            uniqueId   : msg.getProperty('p_uniqueId'),
            status     : statusMapped,
            message    : messageRaw
        ]
    } catch (Exception e) {
        handleError(responseXml, e, messageLogFactory.getMessageLog(msg))
        return [:]
    }
}

/**
 * Anhängen beliebiger Payloads als Attachment (Logging)
 */
void logPayload(Message msg, String name, String content) {
    def log = messageLogFactory.getMessageLog(msg)
    log?.addAttachmentAsString(name, content ?: '', 'text/plain')
}

/**
 * Einheitliche Fehlerbehandlung
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/plain')
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

/**
 * Liefert Datum im Format yyyy-MM-dd
 */
String formatDate(String inDate) {
    if (!inDate) return ''
    // Falls bereits korrekt, einfach zurückgeben
    if (inDate ==~ /\d{4}-\d{2}-\d{2}/) return inDate
    def src = new SimpleDateFormat('yyyy-MM-dd')
    src.format(src.parse(inDate))
}

/**
 * Liefert Zeit im Format HH:mm:ss
 */
String formatTime(String inTime) {
    if (!inTime) return ''
    if (inTime ==~ /\d{2}:\d{2}:\d{2}/) return inTime
    "${inTime}:00"
}

/**
 * Ermittelt TP_OP gem. Regelwerk
 */
String determineTpOp(Message msg) {
    def val = msg.getProperty('p_operacao')?.toString()
    (val && !val.equalsIgnoreCase('placeholder')) ? val : 'I'
}