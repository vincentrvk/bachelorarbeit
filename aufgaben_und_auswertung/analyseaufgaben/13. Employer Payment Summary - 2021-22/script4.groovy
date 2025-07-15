/*****************************************************************************************
 *  Groovy-Skript:  EPS-Importer (UK – eFiling of Employees Payments)
 *
 *  Beschreibung:
 *  1.  Liest den eingehenden Payload (REQUEST) und führt ein Mapping auf das
 *      Ziel-XML-Format „GovTalkMessage“ durch.
 *  2.  Erstellt einen SHA-1-Message-Digest (IRmark) über den Payload
 *      und fügt diesen in das XML ein.
 *  3.  Persistiert den fertigen Payload in einem DataStore  („EPS21-22“).
 *  4.  Ruft den Ziel-Endpunkt mittels HTTP-POST (Basic-Auth) auf.
 *
 *  Modularer Aufbau (siehe Anforderungen):
 *      • prepareTechnicalValues()     – Header/Property Handling
 *      • mapRequest()                – Business-Mapping
 *      • buildIRmark()               – SHA-1 Digest Ermittlung
 *      • writeToDatastore()          – DataStore-Persistierung
 *      • callSetResults()            – HTTP-API-Call
 *      • handleError()               – Zentrales Error-Handling
 *
 *  Autor:  AI-Assistant (Senior Integration Developer)
 *****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory
import java.security.MessageDigest
import groovy.xml.MarkupBuilder
import groovy.xml.XmlUtil

Message processData(Message message) {

    // MessageLog für Monitoring & Attachment Handling
    def msgLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1. Technische Werte ermitteln -------------------------------------------------- */
        def techVals = prepareTechnicalValues(message)

        /* 2. Eingehenden Payload lesen --------------------------------------------------- */
        def inputBody = message.getBody(String) ?: ''
        if (!inputBody.trim()) {
            throw new IllegalArgumentException('Der eingehende Payload ist leer.')
        }

        /* 3. Mapping (ohne IRmark) ------------------------------------------------------- */
        def mappedXmlNoDigest = mapRequest(inputBody, null)

        /* 4. SHA-1 Digest bilden --------------------------------------------------------- */
        def irmark = buildIRmark(mappedXmlNoDigest)

        /* 5. Mapping (mit IRmark) -------------------------------------------------------- */
        def finalPayload = mapRequest(inputBody, irmark)

        /* 6. Write in DataStore ---------------------------------------------------------- */
        writeToDatastore(finalPayload, message, msgLog)

        /* 7. HTTP-POST „Set Results“ ----------------------------------------------------- */
        callSetResults(finalPayload, techVals.requestURL,
                       techVals.requestUser, techVals.requestPassword, msgLog)

        /* 8. Rückgabe -------------------------------------------------------------------- */
        message.setBody(finalPayload)
        return message

    } catch (Exception e) {
        handleError(message.getBody(String), e, msgLog)
        // handleError wirft RuntimeException → return wird nie erreicht
    }
}

/* =======================================================================================
 *  Funktion: prepareTechnicalValues
 *  Liest benötigte Header/Properties oder setzt „placeholder“, sofern nicht vorhanden.
 * =====================================================================================*/
private Map prepareTechnicalValues(Message message) {
    try {
        def props = message.getProperties()
        return [
            requestUser     : props.requestUser     ?: 'placeholder',
            requestPassword : props.requestPassword ?: 'placeholder',
            requestURL      : props.requestURL      ?: 'placeholder'
        ]
    } catch (Exception e) {
        throw new RuntimeException('Fehler beim Lesen der technischen Properties.', e)
    }
}

/* =======================================================================================
 *  Funktion: mapRequest
 *  Führt das Business-Mapping durch und erzeugt das Ziel-XML.
 *  @param sourceXml   – Eingehender Payload als String.
 *  @param irmark      – Optionaler SHA-1 Digest; bei null wird IRmark nicht gesetzt.
 * =====================================================================================*/
private String mapRequest(String sourceXml, String irmark) {
    try {
        def src = new XmlSlurper().parseText(sourceXml)

        StringWriter out = new StringWriter()
        def xml = new MarkupBuilder(out)

        xml.'GovTalkMessage'('xmlns':'http://www.govtalk.gov.uk/CM/envelope') {
            'EnvelopeVersion'()
            'Header' {
                'MessageDetails' {
                    'Class'()
                    'Qualifier'()
                }
                'SenderDetails' {
                    'IDAuthentication' {
                        'SenderID'(src.SenderID.text())
                        'Authentication' {
                            'Method'()
                            'Value'(src.Value.text())
                        }
                    }
                }
            }
            'GovTalkDetails' {
                'ChannelRouting' {
                    'Channel' {
                        'Product'(src.Product.text())
                    }
                    'Timestamp'(src.Timestamp.text())
                }
            }
            'Body' {
                'IRenvelope'('xmlns':'http://www.govtalk.gov.uk/taxation/PAYE/RTI/EmployerPaymentSummary/21-22/1') {
                    'IRheader' {
                        'Keys' {
                            'Key'('Type':'TaxOfficeNumber')
                            'Key'('Type':'TaxOfficeReference')
                        }
                        'PeriodEnd'(src.PeriodEnd.text())
                        // IRmark nur setzen, wenn Wert vorhanden
                        if (irmark) { 'IRmark'(irmark) }
                        'Sender'()
                    }
                    'EmployerPaymentSummary' {
                        'RelatedTaxYear'()
                    }
                }
            }
        }
        return out.toString()
    } catch (Exception e) {
        throw new RuntimeException('Fehler beim Mapping des Payloads.', e)
    }
}

/* =======================================================================================
 *  Funktion: buildIRmark
 *  Ermittelt den SHA-1 Digest (HEX) für den übergebenen String.
 * =====================================================================================*/
private String buildIRmark(String payload) {
    try {
        MessageDigest md = MessageDigest.getInstance('SHA-1')
        byte[] digest   = md.digest(payload.getBytes('UTF-8'))
        return digest.encodeHex().toString().toUpperCase()
    } catch (Exception e) {
        throw new RuntimeException('Fehler beim Erstellen des SHA-1 Digest.', e)
    }
}

/* =======================================================================================
 *  Funktion: writeToDatastore
 *  Persistiert den Payload im DataStore „EPS21-22“
 * =====================================================================================*/
private void writeToDatastore(String payload, Message message, def msgLog) {
    try {
        /* MessageId für EntryId bestimmen */
        def entryId = message.getProperty('SAP_MessageProcessingLogID') ?: UUID.randomUUID().toString()

        def service = new Factory(DataStoreService.class).getService()
        if (service == null) {
            msgLog?.addAttachmentAsString('DatastoreWarning',
                    'DataStoreService konnte nicht initialisiert werden – Persistierung wird übersprungen.',
                    'text/plain')
            return
        }

        /* DataBean vorbereiten */
        DataBean  dBean  = new DataBean()
        dBean.setDataAsArray(payload.getBytes('UTF-8'))

        /* DataConfig vorbereiten */
        DataConfig dCfg  = new DataConfig()
        dCfg.setStoreName('EPS21-22')
        dCfg.setId(entryId)
        dCfg.setOverwrite(true)

        service.put(dBean, dCfg)
    } catch (Exception e) {
        throw new RuntimeException('Fehler beim Schreiben in den DataStore.', e)
    }
}

/* =======================================================================================
 *  Funktion: callSetResults
 *  Führt den HTTP-POST gegen den Ziel-Endpunkt aus.
 * =====================================================================================*/
private void callSetResults(String payload,
                            String url,
                            String user,
                            String pass,
                            def msgLog) {
    HttpURLConnection conn = null
    try {
        conn = (HttpURLConnection) new URL(url).openConnection()
        conn.setRequestMethod('POST')
        conn.setDoOutput(true)
        conn.setRequestProperty('Content-Type', 'application/xml')
        String basicAuth = (user + ':' + pass).bytes.encodeBase64().toString()
        conn.setRequestProperty('Authorization', 'Basic ' + basicAuth)

        conn.getOutputStream().withWriter('UTF-8') { it << payload }

        int rc = conn.getResponseCode()
        msgLog?.setStringProperty('HTTP-ResponseCode', rc.toString())
        // Kein spezielles Response-Handling erforderlich
    } catch (Exception e) {
        throw new RuntimeException("Fehler beim HTTP-Call an URL '${url}'.", e)
    } finally {
        conn?.disconnect()
    }
}

/* =======================================================================================
 *  Funktion: handleError
 *  Zentrales Error-Handling gem. Vorgabe.
 * =====================================================================================*/
private void handleError(String body, Exception e, def messageLog) {
    // Payload als Attachment beilegen
    messageLog?.addAttachmentAsString('ErrorPayload', body ?: '', 'text/xml')
    def errorMsg = "Fehler im EPS-Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}