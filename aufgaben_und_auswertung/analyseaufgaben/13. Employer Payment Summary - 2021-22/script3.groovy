/*************************************************************************
*  Integration :  EPS 21-22 – eFiling of Employees Payments (UK)
*  Author       :  Groovy-Script – Senior Integration Developer
*  Description  :  Erzeugt den EPS-Request, schreibt Original-Payload
*                  in einen DataStore, berechnet den SHA-1-Digest,
*                  ruft den „Set Results“-Service auf und stellt
*                  umfangreiches Error-Handling bereit.
*************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.it.api.asdk.datastore.*
import com.sap.it.api.asdk.runtime.Factory
import java.security.MessageDigest
import groovy.xml.MarkupBuilder
import java.io.StringWriter

/* ============================================================= */
/*  Haupteinstieg                                                */
/* ============================================================= */
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        /* Original-Payload einlesen */
        String originalBody = message.getBody(String)

        /* Header / Properties auslesen bzw. mit Platzhaltern vorbelegen */
        def credentials = prepareCredentials(message)
        String requestUser     = credentials.user
        String requestPassword = credentials.password
        String requestURL      = credentials.url

        /* Mapping durchführen */
        String mappedPayload   = performMapping(originalBody)

        /* SHA-1-Digest bilden und in Payload einfügen */
        String finalPayload    = addMessageDigest(mappedPayload)

        /* Original-Payload in DataStore sichern */
        writeDatastore(originalBody, message)

        /* Set-Results-Service aufrufen */
        int httpRC             = callSetResults(finalPayload, requestURL,
                                                requestUser, requestPassword,
                                                messageLog)

        /* Ergebnis in den Message Context schreiben */
        message.setBody(finalPayload)
        message.setProperty("SetResultsResponseCode", httpRC)

    } catch (Exception e) {
        handleError(message.getBody(String), e, messageLog)
    }
    return message
}

/* ============================================================= */
/*  Funktion: prepareCredentials                                 */
/* ============================================================= */
/*  Liest benötigte Properties & Header aus dem Message-Objekt   */
/*  aus oder setzt den Wert „placeholder“, falls nicht gefunden. */
private Map prepareCredentials(Message message) {
    /* Hilfsclosure zum Ermitteln eines Wertes */
    def getVal = { String key ->
        message.getProperty(key) ?:
        message.getHeader(key, String) ?:
        'placeholder'
    }
    [
        user    : getVal('requestUser'),
        password: getVal('requestPassword'),
        url     : getVal('requestURL')
    ]
}

/* ============================================================= */
/*  Funktion: performMapping                                     */
/* ============================================================= */
/*  Erstellt anhand des Eingangspayloads den geforderten         */
/*  GovTalkMessage-Aufbau.                                       */
private String performMapping(String body) {
    /* Eingangs-Payload parsen (Namespaces werden ignoriert) */
    def src = new XmlSlurper(false, false).parseText(body)

    /* Werte auslesen */
    String senderID  = src.SenderID.text()
    String value     = src.Value.text()
    String product   = src.Product.text()
    String timestamp = src.Timestamp.text()
    String periodEnd = src.PeriodEnd.text()

    /* Ziel-XML mit MarkupBuilder aufbauen */
    def sw = new StringWriter()
    def mb = new MarkupBuilder(sw)
    mb.doubleQuotes = true

    mb.GovTalkMessage(xmlns: 'http://www.govtalk.gov.uk/CM/envelope') {
        EnvelopeVersion()
        Header {
            MessageDetails {
                Class()
                Qualifier()
            }
            SenderDetails {
                IDAuthentication {
                    SenderID(senderID)
                    Authentication {
                        Method()
                        Value(value)
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
            mkp.yieldUnescaped("""
                <IRenvelope xmlns="http://www.govtalk.gov.uk/taxation/PAYE/RTI/EmployerPaymentSummary/21-22/1">
                    <IRheader>
                        <Keys>
                            <Key Type="TaxOfficeNumber"/>
                            <Key Type="TaxOfficeReference"/>
                        </Keys>
                        <PeriodEnd>${periodEnd}</PeriodEnd>
                        <Sender/>
                    </IRheader>
                    <EmployerPaymentSummary>
                        <RelatedTaxYear/>
                    </EmployerPaymentSummary>
                </IRenvelope>
            """.trim())
        }
    }
    return sw.toString()
}

/* ============================================================= */
/*  Funktion: addMessageDigest                                   */
/* ============================================================= */
/*  Berechnet den SHA-1-Digest (Hex) des Payloads und fügt ihn   */
/*  als <IRmark> innerhalb von <IRheader> ein.                   */
private String addMessageDigest(String payload) {
    String digest = computeSHA1(payload.getBytes("UTF-8"))
    /* Regex ersetzt den schließenden IRheader-Tag und fügt IRmark
       unmittelbar davor ein.                                      */
    return payload.replaceFirst(
        '(?s)(<IRheader[^>]*>)(.*?)(</IRheader>)',
        "\$1\$2<IRmark>${digest}</IRmark>\$3"
    )
}

/*  SHA-1-Digest berechnen                                       */
private String computeSHA1(byte[] data) {
    MessageDigest.getInstance('SHA-1')
                 .digest(data)
                 .encodeHex()
                 .toString()
}

/* ============================================================= */
/*  Funktion: writeDatastore                                     */
/* ============================================================= */
/*  Speichert den Original-Payload im DataStore „EPS21-22“.       */
private void writeDatastore(String originalBody, Message message) {
    def service = new Factory(DataStoreService.class).getService()
    if (service) {
        /* DataBean befüllen */
        def dBean = new DataBean()
        dBean.setDataAsArray(originalBody.getBytes("UTF-8"))

        /* DataConfig anlegen */
        def cfg = new DataConfig()
        cfg.setStoreName("EPS21-22")
        cfg.setId(message.getHeader("CamelMessageId", String) ?: UUID.randomUUID().toString())
        cfg.setOverwrite(true)

        service.put(dBean, cfg)
    }
}

/* ============================================================= */
/*  Funktion: callSetResults                                     */
/* ============================================================= */
/*  Führt den HTTP-POST (Basic Auth) zum angegebenen Service aus. */
private int callSetResults(String payload,
                           String url,
                           String user,
                           String password,
                           def   messageLog) {
    if (url == 'placeholder') {
        /* Kein valider Endpunkt vorhanden – überspringen */
        messageLog?.addAttachmentAsString("SetResultsSkipped",
                "URL=placeholder – Aufruf übersprungen.","text/plain")
        return -1
    }

    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection()
    conn.requestMethod  = 'POST'
    conn.doOutput       = true
    conn.setRequestProperty('Content-Type', 'application/xml; charset=UTF-8')
    String basicAuth    = "${user}:${password}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${basicAuth}")

    /* Payload schreiben */
    conn.outputStream.withWriter('UTF-8') { it << payload }

    int rc = conn.responseCode
    messageLog?.addAttachmentAsString("HTTP-Status", rc.toString(), "text/plain")
    return rc
}

/* ============================================================= */
/*  Funktion: handleError                                        */
/* ============================================================= */
/*  Einheitliches Error-Handling mit Ablage des Payloads          */
/*  als Attachment im Monitoring.                                 */
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "", "text/xml")
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}