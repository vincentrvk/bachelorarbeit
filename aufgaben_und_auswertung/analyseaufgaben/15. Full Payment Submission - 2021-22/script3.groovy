/*****************************************************************************************
*  Skript: FPS eFiling UK – Full Payment Submission (21-22)                               *
*  Autor:   Senior Integration Developer                                                  *
*                                                                                         *
*  Beschreibung:                                                                          *
*  1.  Liest eingehenden FPS-Payload und führt das Request-Mapping gemäß Vorgabe aus.     *
*  2.  Bildet einen SHA-1-Nachrichten-Digest (IRmark) und fügt ihn ein.                   *
*  3.  Schreibt das Ergebnis in den DataStore „FPS21-22“ unter der MPL-Message-ID.        *
*  4.  Sendet den Payload via HTTP-POST (Basic-Auth) an die in den Properties hinterlegte *
*      URL („Set Results“).                                                               *
*                                                                                         *
*  Modularer Aufbau:                                                                      *
*     • getContextValues        – ermittelt Properties & Header                          *
*     • buildTargetXml          – erstellt Ziel-XML (Mapping)                            *
*     • computeSha1             – ermittelt SHA-1-Digest                                 *
*     • writeToDataStore        – API-Call DataStoreService.put                          *
*     • sendToSetResults        – API-Call HTTP-POST                                     *
*     • handleError             – zentrales Error-Handling                               *
*****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.StreamingMarkupBuilder
import groovy.xml.XmlUtil
import java.security.MessageDigest
import java.util.UUID
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory

/*****************************************************************************************
*  Haupt-Einstiegspunkt des Groovy-Skripts                                                *
*****************************************************************************************/
Message processData(Message message) {
    // Logging-Instanz für Monitor
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /********** 1. Kontextwerte ermitteln **********/
        def ctx = getContextValues(message)

        /********** 2. Eingehenden Payload parsen **********/
        def inXml = new XmlSlurper().parseText(message.getBody(String) as String)
        def fields = [
                senderID : inXml.SenderID.text(),
                value    : inXml.Value.text(),
                product  : inXml.Product.text(),
                timestamp: inXml.Timestamp.text(),
                periodEnd: inXml.PeriodEnd.text()
        ]

        /********** 3. Mapping ohne IRmark **********/
        String payloadNoMark = buildTargetXml(fields, null)

        /********** 4. SHA-1-Digest berechnen **********/
        String sha1 = computeSha1(payloadNoMark)

        /********** 5. Mapping mit IRmark **********/
        String finalPayload = buildTargetXml(fields, sha1)

        /********** 6. DataStore-Schreibvorgang **********/
        // Message-ID für den DataStore-Eintrag
        def entryId = message.getHeaders()?.get('SAP_MessageProcessingLogID') ?: UUID.randomUUID().toString()
        writeToDataStore("FPS21-22", entryId, finalPayload, messageLog)

        /********** 7. HTTP-POST an „Set Results“ **********/
        sendToSetResults(ctx.url, ctx.user, ctx.password, finalPayload, messageLog)

        /********** 8. Ergebnis in die Exchange zurückschreiben **********/
        message.setBody(finalPayload)
        return message

    } catch (Exception e) {
        // zentrales Error-Handling
        handleError(message.getBody(String) as String, e, messageLog)
    }
}

/*****************************************************************************************
*  getContextValues                                                                      *
*  Ermittelt Properties bzw. Header für User, Password und URL.                          *
*  Fehlt ein Wert, wird „placeholder“ hinterlegt.                                        *
*****************************************************************************************/
private Map<String, String> getContextValues(Message message) {
    def getVal = { String key ->
        def v = message.getProperty(key) ?: message.getHeader(key, String)
        v ? v.toString() : 'placeholder'
    }
    return [
            user    : getVal('requestUser'),
            password: getVal('requestPassword'),
            url     : getVal('requestURL')
    ]
}

/*****************************************************************************************
*  buildTargetXml                                                                        *
*  Erstellt die Ziel-XML-Struktur entsprechend der Mapping-Vorgaben.                     *
*  Parameter:                                                                            *
*     fields  – Map mit den zu verarbeitenden Feldwerten                                 *
*     irMark  – SHA-1-Digest (kann null sein)                                            *
*  Rückgabe: XML-String                                                                  *
*****************************************************************************************/
private String buildTargetXml(Map fields, String irMark) {
    def builder = new StreamingMarkupBuilder()
    builder.encoding = 'UTF-8'
    def xmlClosure = {
        mkp.xmlDeclaration()
        mkp.declareNamespace("" : 'http://www.govtalk.gov.uk/CM/envelope')

        'GovTalkMessage' {
            'EnvelopeVersion'('')
            'Header' {
                'MessageDetails' {
                    'Class'('')
                    'Qualifier'('')
                }
                'SenderDetails' {
                    'IDAuthentication' {
                        'SenderID'(fields.senderID)
                        'Authentication' {
                            'Method'('')
                            'Value'(fields.value)
                        }
                    }
                }
            }
            'GovTalkDetails' {
                'ChannelRouting' {
                    'Channel' {
                        'Product'(fields.product)
                    }
                    'Timestamp'(fields.timestamp)
                }
            }
            'Body' {
                'IRenvelope'('xmlns': 'http://www.govtalk.gov.uk/taxation/PAYE/RTI/FullPaymentSubmission/21-22/1') {
                    'IRheader' {
                        'Keys' {
                            'Key'(Type: 'TaxOfficeNumber', '')
                            'Key'(Type: 'TaxOfficeReference', '')
                        }
                        if (irMark) {
                            'IRmark'(irMark)
                        }
                        'PeriodEnd'(fields.periodEnd)
                        'Sender'('')
                    }
                    'FullPaymentSubmission' {
                        'RelatedTaxYear'('')
                    }
                }
            }
        }
    }
    return XmlUtil.serialize(builder.bind(xmlClosure))
}

/*****************************************************************************************
*  computeSha1                                                                           *
*  Berechnet den SHA-1-Hash eines Strings und gibt ihn hex-kodiert zurück.               *
*****************************************************************************************/
private String computeSha1(String data) {
    MessageDigest md = MessageDigest.getInstance('SHA-1')
    byte[] digest = md.digest(data.getBytes('UTF-8'))
    return digest.encodeHex().toString()
}

/*****************************************************************************************
*  writeToDataStore                                                                      *
*  Schreibt den Payload in den definierten DataStore.                                    *
*  Error-Handling via handleError().                                                     *
*****************************************************************************************/
private void writeToDataStore(String storeName, String entryId, String payload, def messageLog) {
    try {
        def service = new Factory(DataStoreService.class).getService()
        if (service) {
            def dBean = new DataBean()
            dBean.setDataAsArray(payload.getBytes('UTF-8'))

            def config = new DataConfig()
            config.setStoreName(storeName)
            config.setId(entryId)
            config.setOverwrite(true)

            service.put(dBean, config)
        } else {
            messageLog?.addAttachmentAsString('DataStoreError', 'DataStoreService not verfügbar', 'text/plain')
        }
    } catch (Exception e) {
        handleError(payload, e, messageLog)
    }
}

/*****************************************************************************************
*  sendToSetResults                                                                      *
*  Führt den HTTP-POST gegen die bereitgestellte URL aus (Basic-Auth).                   *
*  Bei placeholder-URL wird der Call übersprungen.                                       *
*****************************************************************************************/
private void sendToSetResults(String url, String user, String password, String payload, def messageLog) {
    try {
        if ('placeholder'.equalsIgnoreCase(url)) {
            messageLog?.addAttachmentAsString('HTTP-Info', 'HTTP-Call übersprungen – placeholder URL', 'text/plain')
            return
        }

        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
        conn.requestMethod = 'POST'
        conn.doOutput = true
        conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
        String auth = "${user}:${password}".bytes.encodeBase64().toString()
        conn.setRequestProperty('Authorization', "Basic $auth")

        conn.outputStream.withWriter('UTF-8') { it << payload }
        int rc = conn.responseCode
        messageLog?.addAttachmentAsString('HTTP-ResponseCode', rc.toString(), 'text/plain')

    } catch (Exception e) {
        handleError(payload, e, messageLog)
    }
}

/*****************************************************************************************
*  handleError                                                                           *
*  Zentrales Error-Handling – speichert den fehlerhaften Payload als Attachment und      *
*  wirft eine RuntimeException mit sprechender Fehlermeldung.                            *
*****************************************************************************************/
private void handleError(String body, Exception e, def messageLog) {
    // Payload für spätere Analyse an das MPL anhängen
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}