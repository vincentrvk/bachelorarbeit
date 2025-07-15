/*****************************************************************************************
 *  SAP Cloud Integration – Groovy-Skript
 *  Earlier Year Update (EYU) 17/18 – eFiling of Employees Payments UK
 *
 *  Erfüllt folgende Anforderungen:
 *   • Modularer Aufbau (Functions siehe unten)
 *   • Vollständiges Logging & Error-Handling
 *   • Mapping gem. Vorgabe inkl. SHA-1-IRmark
 *   • Schreiben in DataStore „EYU17-18“
 *   • Aufruf „Set Results“ (HTTP POST, Basic-Auth)
 *
 *  Autor:  (Senior Integration Developer)
 *****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.gateway.ip.core.customdev.util.MessageLog
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory

import groovy.xml.StreamingMarkupBuilder
import groovy.xml.XmlUtil

import java.security.MessageDigest
import java.net.HttpURLConnection
import java.net.URL
import java.util.UUID

// ========================================================================================
// Main Function – CPI Entry Point
// ========================================================================================
Message processData(Message message) {
    final MessageLog messageLog = messageLogFactory.getMessageLog(message)
    final String originalBody = message.getBody(String)                       // Eingehender Payload

    try {
        // 1. Properties / Header lesen
        Map<String, String> cfg = readPropsAndHeaders(message, messageLog)

        // 2. Mapping ausführen (ohne IRmark)
        String mappedXml = mapPayload(originalBody, messageLog)

        // 3. SHA-1 Digest bilden und IRmark einfügen
        String payloadWithDigest = addMessageDigest(mappedXml, messageLog)

        // 4. Payload in DataStore schreiben
        writeDataStore(payloadWithDigest, message, messageLog)

        // 5. „Set Results“ Aufruf ausführen
        callSetResultsAPI(payloadWithDigest, cfg, messageLog)

        // 6. Payload im CPI-Nachrichtenkörper aktualisieren
        message.setBody(payloadWithDigest)
        return message
    } catch (Exception e) {
        handleError(originalBody, e, messageLog)                              // delegiert Fehlerwurf
        return message                                                        // niemals erreicht
    }
}

// ========================================================================================
// Function: readPropsAndHeaders
// Liest Properties & Header oder setzt Placeholder.
// ========================================================================================
private Map<String, String> readPropsAndHeaders(Message msg, MessageLog log) {
    try {
        def props      = msg.getProperties()
        String user     = props['requestUser']     ?: 'placeholder'
        String pwd      = props['requestPassword'] ?: 'placeholder'
        String url      = props['requestURL']      ?: 'placeholder'

        log?.addAttachmentAsString('PropertyInfo',
                "User: ${user}\nURL : ${url}", 'text/plain')
        return [user: user, password: pwd, url: url]
    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Lesen der Properties: ${e.message}", e)
    }
}

// ========================================================================================
// Function: mapPayload
// Erstellt Ziel-XML gem. Mapping-Vorgabe (ohne IRmark!)
// ========================================================================================
private String mapPayload(String sourceXml, MessageLog log) {
    try {
        def src = new XmlSlurper().parseText(sourceXml)

        // Eingabewerte
        String senderID   = src.'SenderID'.text()
        String value      = src.'Value'.text()
        String product    = src.'Product'.text()
        String timestamp  = src.'Timestamp'.text()
        String periodEnd  = src.'PeriodEnd'.text()

        // Platzhalter für späteren Digest
        final String IR_MARK_TOKEN = 'IRMARKTOKEN'

        // Aufbau Ziel-XML
        StreamingMarkupBuilder smb = new StreamingMarkupBuilder()
        smb.encoding = 'UTF-8'

        def target = {
            mkp.declareNamespace('', 'http://www.govtalk.gov.uk/CM/envelope')
            'GovTalkMessage' {
                'EnvelopeVersion'('')
                'Header' {
                    'MessageDetails' {
                        'Class'('')
                        'Qualifier'('')
                        'CorrelationID'('')
                        'GatewayTimestamp'('')
                    }
                    'SenderDetails' {
                        'IDAuthentication' {
                            'SenderID'(senderID)
                            'Authentication' {
                                'Method'('')
                                'Value'(value)
                            }
                        }
                    }
                }
                'GovTalkDetails' {
                    'ChannelRouting' {
                        'Channel' {
                            'Product'(product)
                        }
                        'Timestamp'(timestamp)
                    }
                }
                'Body' {
                    'IRenvelope'('xmlns':'http://www.govtalk.gov.uk/taxation/PAYE/RTI/EarlierYearUpdate/16-17/1') {
                        'IRheader' {
                            'Keys' {
                                'Key'(' Type':'TaxOfficeNumber')
                                'Key'(' Type':'TaxOfficeReference')
                            }
                            'PeriodEnd'(periodEnd)
                            'Sender'('')
                            'IRmark'(IR_MARK_TOKEN)       // wird später ersetzt
                        }
                        'EarlierYearUpdate' {
                            'RelatedTaxYear'('')
                        }
                    }
                }
            }
        }

        String result = smb.bind(target).toString()
        log?.addAttachmentAsString('MappedPayload', result, 'text/xml')
        return result
    } catch (Exception e) {
        throw new RuntimeException("Fehler im Mapping: ${e.message}", e)
    }
}

// ========================================================================================
// Function: addMessageDigest
// Bildet SHA-1 Digest über den übergebenen Payload (ohne IRmark) und ersetzt Platzhalter.
// ========================================================================================
private String addMessageDigest(String xmlWithoutDigest, MessageLog log) {
    try {
        // SHA-1 Digest ermitteln
        byte[] digestBytes = MessageDigest.getInstance('SHA-1')
                                          .digest(xmlWithoutDigest.getBytes('UTF-8'))
        String digestHex   = digestBytes.collect { String.format('%02x', it) }.join()

        // IRmark einsetzen
        String finalXml = xmlWithoutDigest.replace('IRMARKTOKEN', digestHex)

        log?.addAttachmentAsString('PayloadWithDigest', finalXml, 'text/xml')
        return finalXml
    } catch (Exception e) {
        throw new RuntimeException("Fehler bei Digest-Berechnung: ${e.message}", e)
    }
}

// ========================================================================================
// Function: writeDataStore
// Schreibt Payload in DataStore „EYU17-18“. EntryId = CamelMessageId (Fallback UUID)
// ========================================================================================
private void writeDataStore(String payload, Message msg, MessageLog log) {
    try {
        DataStoreService svc = new Factory(DataStoreService.class).getService()
        if (svc == null) throw new RuntimeException('DataStoreService konnte nicht instanziiert werden.')

        DataBean   bean   = new DataBean()
        bean.setDataAsArray(payload.getBytes('UTF-8'))

        DataConfig cfg    = new DataConfig()
        cfg.setStoreName('EYU17-18')
        cfg.setId(msg.getHeaders()['CamelMessageId'] ?: UUID.randomUUID().toString())
        cfg.setOverwrite(true)

        svc.put(bean, cfg)
        log?.addAttachmentAsString('DataStoreWrite',
                "Payload als EntryId '${cfg.getId()}' gespeichert.", 'text/plain')
    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Schreiben in den DataStore: ${e.message}", e)
    }
}

// ========================================================================================
// Function: callSetResultsAPI
// Führt HTTP-POST (Basic Auth) ohne Response-Mapping aus.
// ========================================================================================
private void callSetResultsAPI(String payload, Map<String, String> cfg, MessageLog log) {
    HttpURLConnection conn = null
    try {
        conn = (HttpURLConnection) new URL(cfg.url).openConnection()
        conn.setRequestMethod('POST')
        conn.setDoOutput(true)
        conn.setRequestProperty('Content-Type', 'application/xml')
        String basicAuth = "${cfg.user}:${cfg.password}".getBytes('UTF-8').encodeBase64().toString()
        conn.setRequestProperty('Authorization', "Basic ${basicAuth}")

        conn.outputStream.withWriter('UTF-8') { it << payload }
        int rc = conn.responseCode
        log?.addAttachmentAsString('HTTP-ResponseCode', rc.toString(), 'text/plain')
    } catch (Exception e) {
        throw new RuntimeException("Fehler beim HTTP-Aufruf: ${e.message}", e)
    } finally {
        conn?.disconnect()
    }
}

// ========================================================================================
// Function: handleError
// Zentrales Error-Handling – wirft RuntimeException weiter, Payload als Attachment.
// ========================================================================================
private void handleError(String body, Exception e, MessageLog messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    String errorMsg = "Fehler im Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}