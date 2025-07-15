/*************************************************************************
*  Groovy-Skript  :  EYU_Request_Handling.groovy
*  Beschreibung   :  Erstellt den Request für HMRC-EYU, erzeugt IRmark,
*                    schreibt das Ergebnis in einen DataStore und ruft
*                    den HMRC-Endpunkt via HTTP-POST auf.
*************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.*
import groovy.xml.*
import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.net.HttpURLConnection
import java.net.URL

/*************************************************************************
*  ENTRYPOINT
*************************************************************************/
Message processData(Message message) {

    // MessageLog zum Mitschreiben – kann im Monitoring eingesehen werden
    def messageLog = messageLogFactory.getMessageLog(message)
    try {

        /*------------------------------------------------------------
        *  1. Eingehenden Payload lesen & Properties/Header verwalten
        *-----------------------------------------------------------*/
        String inPayload   = message.getBody(String)
        def    parameters  = getConnectivityParameters(message, messageLog)

        /*------------------------------------------------------------
        *  2. Mapping durchführen
        *-----------------------------------------------------------*/
        String mappedXml   = mapRequest(inPayload, messageLog)

        /*------------------------------------------------------------
        *  3. IRmark ermitteln und in das XML einfügen
        *-----------------------------------------------------------*/
        String irMark      = createIRmark(mappedXml)
        String finalXml    = insertIRmark(mappedXml, irMark, messageLog)

        /*------------------------------------------------------------
        *  4. Payload im DataStore ablegen
        *-----------------------------------------------------------*/
        String msgId       = (message.getHeaders()?.get('CamelMessageId')
                           ?: UUID.randomUUID().toString())
        writeToDatastore(finalXml, "EYU17-18", msgId, messageLog)

        /*------------------------------------------------------------
        *  5. HMRC-Endpunkt aufrufen
        *-----------------------------------------------------------*/
        performApiCall(finalXml,
                       parameters.url,
                       parameters.user,
                       parameters.pass,
                       messageLog)

        /*------------------------------------------------------------
        *  6. Ergebnis in den Message-Body schreiben
        *-----------------------------------------------------------*/
        message.setBody(finalXml)
        return message

    } catch (Exception e) {
        // zentrales Error-Handling
        handleError(message.getBody(String) as String, e, messageLog)
    }
}

/*************************************************************************
*  Function: getConnectivityParameters
*  Zweck   : Liest Properties & Header aus dem Message-Objekt oder
*            belegt sie mit "placeholder"
*************************************************************************/
private Map getConnectivityParameters(Message message, def messageLog) {
    try {
        String user = (message.getProperty('requestUser')
                    ?: message.getHeader('requestUser', String.class)
                    ?: 'placeholder')
        String pass = (message.getProperty('requestPassword')
                    ?: message.getHeader('requestPassword', String.class)
                    ?: 'placeholder')
        String url  = (message.getProperty('requestURL')
                    ?: message.getHeader('requestURL', String.class)
                    ?: 'placeholder')

        return [user: user, pass: pass, url: url]

    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Auslesen der Konnektivitäts-Parameter", e)
    }
}

/*************************************************************************
*  Function: mapRequest
*  Zweck   : Mapping INPUT  ➜  OUTPUT laut Vorgabe
*************************************************************************/
private String mapRequest(String sourceXml, def messageLog) {
    try {
        // Namespace des Eingangs-Dokuments
        final String cmNs = "http://www.govtalk.gov.uk/CM/envelope"
        def slurper       = new XmlSlurper()
        def inRoot        = slurper.parseText(sourceXml)

        // Helper für namespace-unabhängigem Zugriff
        def getVal = { String tag ->
            inRoot.'**'.find { it.name()?.localPart == tag }?.text()?.trim()
        }

        String senderId  = getVal('SenderID')
        String authVal   = getVal('Value')
        String product   = getVal('Product')
        String timestamp = getVal('Timestamp')
        String periodEnd = getVal('PeriodEnd')

        // Namespace für RTI-Bereich
        final String rtiNs = "http://www.govtalk.gov.uk/taxation/PAYE/RTI/EarlierYearUpdate/16-17/1"

        /*--------------------------------------------------------
        *  XML-Aufbau mit MarkupBuilder
        *-------------------------------------------------------*/
        def sw  = new StringWriter()
        def xml = new MarkupBuilder(sw)
        xml.doubleQuotes = true

        xml.GovTalkMessage(xmlns: cmNs) {
            EnvelopeVersion('')
            Header {
                MessageDetails {
                    Class('')
                    Qualifier('')
                    CorrelationID('')
                    GatewayTimestamp('')
                }
                SenderDetails {
                    IDAuthentication {
                        SenderID(senderId)
                        Authentication {
                            Method('')
                            Value(authVal)
                        }
                    }
                }
            }
            GovTalkDetails {
                ChannelRouting {
                    Channel {
                        Product(product)
                    }
                    Timestamp(timestamp)
                }
            }
            Body {
                mkp.declareNamespace("" : rtiNs)      // Default-NS für IRenvelope
                IRenvelope {
                    IRheader {
                        Keys {
                            Key(Type: 'TaxOfficeNumber', '')
                            Key(Type: 'TaxOfficeReference', '')
                        }
                        PeriodEnd(periodEnd)
                        Sender('')
                    }
                    EarlierYearUpdate {
                        RelatedTaxYear('')
                    }
                }
            }
        }
        return sw.toString()

    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Request-Mapping", e)
    }
}

/*************************************************************************
*  Function: createIRmark
*  Zweck   : Bildet den SHA1-Hash (Hex, UpperCase) über den Payload
*************************************************************************/
private String createIRmark(String payload) {
    MessageDigest md = MessageDigest.getInstance("SHA-1")
    byte[] hashBytes = md.digest(payload.getBytes(StandardCharsets.UTF_8))
    return hashBytes.collect { String.format("%02x", it) }.join().toUpperCase()
}

/*************************************************************************
*  Function: insertIRmark
*  Zweck   : Fügt den berechneten Hash in das XML ein
*************************************************************************/
private String insertIRmark(String xml, String irMark, def messageLog) {
    try {
        def parser = new XmlParser()
        def root   = parser.parseText(xml)

        // passenden IRheader suchen (Namespace-agnostisch)
        def irHeader = root.'**'.find { it.name()?.getLocalPart() == 'IRheader' }
        if (irHeader == null) {
            throw new RuntimeException("IRheader-Element nicht gefunden")
        }

        // Namespace des übergeordneten IRenvelope ermitteln
        String rtiNs = irHeader.name().getNamespaceURI()

        // IRmark als erstes Kind einfügen
        new Node(irHeader,
                 new QName(rtiNs, 'IRmark'),
                 irMark)

        return XmlUtil.serialize(root)

    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Einfügen des IRmark", e)
    }
}

/*************************************************************************
*  Function: writeToDatastore
*  Zweck   : Schreibt den Payload in den angegebenen DataStore
*************************************************************************/
private void writeToDatastore(String payload,
                              String storeName,
                              String entryId,
                              def messageLog) {
    try {
        def service = new Factory(DataStoreService.class).getService()
        if (service == null) {
            messageLog?.addAttachmentAsString("DataStoreWarning",
                    "DataStoreService nicht verfügbar", "text/plain")
            return
        }

        DataBean   dBean   = new DataBean()
        dBean.setDataAsArray(payload.getBytes(StandardCharsets.UTF_8))

        DataConfig config  = new DataConfig()
        config.setStoreName(storeName)
        config.setId(entryId)
        config.setOverwrite(true)

        service.put(dBean, config)
    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Schreiben in den DataStore", e)
    }
}

/*************************************************************************
*  Function: performApiCall
*  Zweck   : Führt den HTTP-POST (Basic Auth) zum HMRC-Endpunkt aus
*************************************************************************/
private void performApiCall(String payload,
                            String targetUrl,
                            String user,
                            String pass,
                            def messageLog) {
    try {
        if (targetUrl == 'placeholder') {
            // Kein echter Aufruf wenn keine Ziel-URL vorhanden
            messageLog?.addAttachmentAsString("APICallSkipped",
                    "Kein Request ausgeführt – URL = placeholder", "text/plain")
            return
        }

        URL url               = new URL(targetUrl)
        HttpURLConnection con = (HttpURLConnection) url.openConnection()
        con.setRequestMethod("POST")
        con.setDoOutput(true)
        con.setRequestProperty("Content-Type", "application/xml")
        String basicAuth      = "${user}:${pass}".bytes.encodeBase64().toString()
        con.setRequestProperty("Authorization", "Basic " + basicAuth)

        // Payload schreiben
        con.outputStream.withWriter("UTF-8") { it << payload }
        int responseCode = con.responseCode
        messageLog?.setStringProperty("HTTP_ResponseCode", responseCode.toString())

    } catch (Exception e) {
        throw new RuntimeException("Fehler beim HTTP-Request", e)
    }
}

/*************************************************************************
*  Function: handleError
*  Zweck   : Zentrales Error-Handling (Attachment + aussagekräftige Meldung)
*************************************************************************/
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "", "text/xml")
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}