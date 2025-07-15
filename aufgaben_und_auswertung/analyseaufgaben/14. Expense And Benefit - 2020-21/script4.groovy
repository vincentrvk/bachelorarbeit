/*****************************************************************************************
 *  eFiling of Employees Payments – UK  (Expense & Benefits)
 *  Groovy-Script für SAP Cloud Integration
 *
 *  Anforderungen:
 *   • Modularer Aufbau (separate Funktionen siehe unten)
 *   • Vollständige Kommentierung (DE)
 *   • Aussagekräftiges Error-Handling (Payload als Attachment)
 *   • SHA-1-Digest   (Element IRmark)
 *   • Schreiben in DataStore  (Name: EXB20-21, Key = MessageID)
 *   • HTTP-POST  (Basic-Auth, URL aus Property requestURL)
 *****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import java.security.MessageDigest
import groovy.xml.*

// --- Einstieg -----------------------------------------------------------
Message processData(Message message) {

    def messageLog = messageLogFactory?.getMessageLog(message)

    try {
        /* 1. Konfiguration laden -------------------------------------------------------- */
        def cfg = getConfigValues(message, messageLog)       // Map mit user, pw, url

        /* 2. Eingehenden Payload mappen ------------------------------------------------ */
        String sourcePayload = message.getBody(String) ?: ''
        String mappedPayload = mapRequestPayload(sourcePayload, messageLog)

        /* 3. SHA-1 Digest berechnen und einfügen --------------------------------------- */
        String digest      = calculateDigest(mappedPayload)
        String finalPayload = insertDigest(mappedPayload, digest, messageLog)

        /* 4. Payload in DataStore schreiben ------------------------------------------- */
        writeToDataStore(finalPayload, message, messageLog)

        /* 5. HTTP-POST (Set Results) --------------------------------------------------- */
        performHttpPost(finalPayload, cfg, messageLog)

        /* 6. Payload als neuen Body setzen -------------------------------------------- */
        message.setBody(finalPayload)
        return message

    } catch (Exception e) {
        handleError(message.getBody(String) ?: '', e, messageLog)
        // handleError wirft eine RuntimeException, return wird nicht erreicht
    }
}

/* ===============================================================================
 *  Hilfsfunktionen
 * =============================================================================== */

/*-----------------------------------------------------------
 *  Lese Header & Properties und setze Default „placeholder“
 *-----------------------------------------------------------*/
def Map getConfigValues(Message message, def log) {
    def p = message.getProperties()

    String user = p?.requestUser     ?: 'placeholder'
    String pw   = p?.requestPassword ?: 'placeholder'
    String url  = p?.requestURL      ?: 'placeholder'

    log?.addAttachmentAsString('ConfigValues',
            "user=${user}\nurl=${url}", 'text/plain')

    return [user: user, pw: pw, url: url]
}

/*-----------------------------------------------------------
 *  Mapping des Eingangs-XML auf GovTalkMessage-Struktur
 *   (ohne IRmark!)
 *-----------------------------------------------------------*/
def String mapRequestPayload(String inputXml, def log) {

    // Quelle parsen
    def src = new XmlSlurper().parseText(inputXml)
    final NS_ENV = 'http://www.govtalk.gov.uk/CM/envelope'

    // Ziel mittels StreamingMarkupBuilder erzeugen
    def smb = new StreamingMarkupBuilder()
    smb.encoding = 'UTF-8'

    def result = smb.bind {
        mkp.xmlDeclaration()
        'GovTalkMessage'('xmlns': NS_ENV) {
            'EnvelopeVersion'('')                          // leer
            'Header' {
                'MessageDetails' {
                    'Class'('')
                    'Qualifier'('')
                }
                'SenderDetails' {
                    'IDAuthentication' {
                        'SenderID'(src.'SenderID'.text())
                        'Authentication' {
                            'Method'('')
                            'Value'(src.'Value'.text())
                        }
                    }
                }
            }
            'GovTalkDetails' {
                'ChannelRouting' {
                    'Channel' {
                        'Product'(src.'Product'.text())
                    }
                    'Timestamp'(src.'Timestamp'.text())
                }
            }
            'Body' {
                // Namespace der IRenvelope gemäß Vorgabe
                'IRenvelope'('xmlns': 'http://www.govtalk.gov.uk/taxation/EXB/20-21/1') {
                    'IRheader' {
                        'Keys' {
                            'Key'('Type': 'TaxOfficeNumber', '')
                            'Key'('Type': 'TaxOfficeReference', '')
                        }
                        'PeriodEnd'('')
                        'DefaultCurrency'(src.'DefaultCurrency'.text())
                        'Sender'('')
                        // IRmark wird später ergänzt
                    }
                    'ExpensesAndBenefits'('')
                }
            }
        }
    }.toString()

    log?.addAttachmentAsString('MappedPayload_ohne_IRmark', result, 'text/xml')
    return result
}

/*-----------------------------------------------------------
 * Berechne SHA-1-Digest des übergebenen Strings
 *-----------------------------------------------------------*/
def String calculateDigest(String payload) {
    MessageDigest md = MessageDigest.getInstance('SHA-1')
    byte[] digestBytes = md.digest(payload.getBytes('UTF-8'))
    // Hex-String erzeugen
    digestBytes.collect { String.format('%02x', it) }.join()
}

/*-----------------------------------------------------------
 * Füge den Digest als IRmark ein
 *-----------------------------------------------------------*/
def String insertDigest(String payload, String digest, def log) {

    def slurper = new XmlSlurper().parseText(payload)
    // Navigate zum IRheader
    def irHeader = slurper.Body.IRenvelope.IRheader[0]
    irHeader.appendNode { 'IRmark'(digest) }

    def finalXml = XmlUtil.serialize(slurper)
    log?.addAttachmentAsString('FinalPayload_mit_IRmark', finalXml, 'text/xml')
    return finalXml
}

/*-----------------------------------------------------------
 *  Schreibe Payload in DataStore (ASDK-Service)
 *-----------------------------------------------------------*/
def void writeToDataStore(String payload, Message message, def log) {

    def service = new Factory(com.sap.it.api.asdk.datastore.DataStoreService).getService()
    if (service == null) {
        throw new RuntimeException('DataStoreService konnte nicht instanziiert werden.')
    }

    // DataBean befüllen
    def dBean = new com.sap.it.api.asdk.datastore.DataBean()
    dBean.setDataAsArray(payload.getBytes('UTF-8'))

    // Konfiguration
    def dCfg = new com.sap.it.api.asdk.datastore.DataConfig()
    dCfg.setStoreName('EXB20-21')
    dCfg.setId(message.getMessageId())
    dCfg.setOverwrite(true)

    service.put(dBean, dCfg)
    log?.addAttachmentAsString('DataStoreInfo',
            "Store: EXB20-21\nKey  : ${message.getMessageId()}", 'text/plain')
}

/*-----------------------------------------------------------
 *  Aufruf des externen „Set Results“-Endpunktes
 *-----------------------------------------------------------*/
def void performHttpPost(String payload, Map cfg, def log) {

    if (cfg.url == 'placeholder') {
        log?.addAttachmentAsString('HTTP-Skip',
                'requestURL = placeholder  ➔  HTTP-POST übersprungen.', 'text/plain')
        return
    }

    URL url = new URL(cfg.url)
    HttpURLConnection conn = (HttpURLConnection) url.openConnection()
    conn.setRequestMethod('POST')
    conn.setDoOutput(true)
    conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')

    // Basic-Auth Header setzen
    String auth = "${cfg.user}:${cfg.pw}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${auth}")

    // Body schreiben
    conn.outputStream.withWriter('UTF-8') { it << payload }

    int rc = conn.responseCode
    log?.setStringProperty('HTTP-Response-Code', rc.toString())
}

/*-----------------------------------------------------------
 *  Zentrales Error-Handling (Snippet übernommen)
 *-----------------------------------------------------------*/
def handleError(String body, Exception e, def messageLog) {
    // Logging im Monitoring (Name, Inhalt, Typ)
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}