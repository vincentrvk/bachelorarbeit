/*****************************************************************************************
 *  Groovy‐Skript für die Integration S/4HANA Cloud  ↔  CCH Sure Tax – Geocode Batch
 *  Autor: ChatGPT (Senior-Integration-Developer)
 *
 *  Vorgaben:
 *    – Modularer Aufbau (eigene Funktionen pro Aufgabe)
 *    – Deutschsprachige Kommentare
 *    – Umfassendes Error-Handling mit aussagekräftigem Logging
 *    – Anhängen aller relevanten Payloads an die Message (addAttachmentAsString)
 *****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.StreamingMarkupBuilder
import java.net.HttpURLConnection
import java.util.Base64

/*****************************************************************************************
 *  Haupteinstiegspunkt des Skripts
 *****************************************************************************************/
Message processData(Message message)
{
    // Message-Log holen (für Anhänge & Logs)
    def messageLog = messageLogFactory.getMessageLog(message)

    try
    {
        /* 1) Logging des eingehenden Payloads ********************************************/
        String incomingPayload = message.getBody(String) ?: ''
        addLogAttachment(messageLog, 'IncomingPayload', incomingPayload)

        /* 2) Header & Properties einsammeln *********************************************/
        def ctx = readContext(message)

        /* 3) Request-Mapping ************************************************************/
        String requestXml = mapRequest(incomingPayload, ctx)
        addLogAttachment(messageLog, 'RequestPayload', requestXml)

        /* 4) Aufruf des SureTax-Endpunktes **********************************************/
        String responseXml = callSureTaxApi(ctx, requestXml, messageLog)
        addLogAttachment(messageLog, 'ResponsePayload', responseXml)

        /* 5) Response-Mapping ***********************************************************/
        String mappedResponse = mapResponse(responseXml, ctx)
        addLogAttachment(messageLog, 'MappedResponsePayload', mappedResponse)

        /* 6) Ergebnis in den Body zurückgeben *******************************************/
        message.setBody(mappedResponse)
        return message
    }
    catch (Exception e)
    {
        // Zentrale Fehlerbehandlung
        handleError(message, e, messageLog)
        return message    // wird nie erreicht, da handleError Exception wirft
    }
}

/*****************************************************************************************
 *  Kontext (Header & Properties) einsammeln oder Placeholder setzen
 *****************************************************************************************/
private Map readContext(Message message)
{
    Map ctx = [:]

    // Properties
    ctx.sureTaxUsername      = message.getProperty('sureTaxUsername')     ?: 'placeholder'
    ctx.sureTaxPassword      = message.getProperty('sureTaxPassword')     ?: 'placeholder'
    ctx.sureTaxURL           = message.getProperty('sureTaxURL')          ?: 'placeholder'
    ctx.exchangejcdunifyind  = message.getProperty('exchangejcdunifyind') ?: 'placeholder'

    // Header können hier – falls benötigt – analog abgefragt werden

    return ctx
}

/*****************************************************************************************
 *  Request-Mapping: SAP-Payload  ➜  SureTax-Request
 *****************************************************************************************/
private String mapRequest(String sourceXml, Map ctx)
{
    def src  = new XmlSlurper().parseText(sourceXml)

    // Validierung: COUNTRY ist Pflichtfeld
    src.LOCATION_SIMPLE.each {
        if (!it.COUNTRY.text())
            throw new IllegalArgumentException('Validation failed – COUNTRY ist obligatorisch.')
    }

    // StreamingMarkupBuilder erzeugt performantes XML
    def smb = new StreamingMarkupBuilder()
    smb.encoding = 'UTF-8'

    def req = {
        mkp.declareNamespace(ns3: 'http://soa.noventic.com/GeocodeService/GeocodeService-V1')
        'ns3:BatchGetGeocode' {
            'ns3:request' {
                'ns3:ClientNumber'(ctx.sureTaxUsername)
                'ns3:DataYear'(new Date().format('yyyy'))
                'ns3:LocationList' {
                    src.LOCATION_SIMPLE.each { loc ->
                        def seq  = loc.SEQUENCE_NUM.text().trim()
                        def zip  = resolveZip(loc.ZIPCODE.text(), loc.COUNTRY.text())
                        def addr = (loc.STREET_ADDRESS_SIMPLE.STREET.text() ?: '').trim()

                        'ns3:Location' {
                            'ns3:SequenceNum'(seq)
                            'ns3:Zipcode'(zip)
                            'ns3:AddressLine1'(addr)
                        }
                    }
                }
            }
        }
    }

    return smb.bind(req).toString()
}

/*****************************************************************************************
 *  ZIP-Ermittlung gem. Regelwerk
 *****************************************************************************************/
private String resolveZip(String zipcode, String country)
{
    if (!zipcode) return ''
    if (country?.equalsIgnoreCase('US') || country?.equalsIgnoreCase('USA'))
    {
        return zipcode.trim()
    }
    else
    {
        // Erste Token vor „-“, danach trimmen
        return zipcode.tokenize('-').first().trim()
    }
}

/*****************************************************************************************
 *  HTTP-Aufruf des SureTax-Services
 *****************************************************************************************/
private String callSureTaxApi(Map ctx, String requestBody, def messageLog)
{
    HttpURLConnection con = null
    try
    {
        URL url = new URL(ctx.sureTaxURL)
        con = (HttpURLConnection) url.openConnection()
        con.with {
            doOutput       = true
            requestMethod  = 'POST'
            setRequestProperty('Content-Type', 'text/xml; charset=UTF-8')

            // Basic-Auth Header
            String authStr = "${ctx.sureTaxUsername}:${ctx.sureTaxPassword}"
            String authEnc = Base64.encoder.encodeToString(authStr.bytes)
            setRequestProperty('Authorization', "Basic $authEnc")

            // Payload schreiben
            outputStream.withWriter('UTF-8') { it << requestBody }
        }

        int rc = con.responseCode
        if (rc != HttpURLConnection.HTTP_OK)
        {
            String errBody = con.errorStream?.getText('UTF-8') ?: ''
            messageLog?.addAttachmentAsString('SureTaxErrorResponse', errBody, 'text/plain')
            throw new IllegalStateException("SureTax API antwortete mit HTTP ${rc}")
        }

        return con.inputStream.getText('UTF-8')
    }
    finally
    {
        con?.disconnect()
    }
}

/*****************************************************************************************
 *  Response-Mapping: SureTax-Response  ➜  SAP-Zielschema
 *****************************************************************************************/
private String mapResponse(String responseXml, Map ctx)
{
    def resp = new XmlSlurper().parseText(responseXml)

    def smb = new StreamingMarkupBuilder()
    smb.encoding = 'UTF-8'

    def out = {
        mkp.declareNamespace(ns2: 'http://sap.com/xi/FotETaxUS')
        'ns2:TAX_JUR_REDEFINE_RECEIVE' {
            // Alle GeocodeResponse-Knoten verarbeiten
            resp.'**'.findAll { it.name() == 'GeocodeResponse' }.each { gr ->
                String geocode      = gr.Geocode.text()
                String seqNum       = gr.SequenceNum.text()
                String responseCode = gr.ResponseCode.text()
                String errorMsg     = gr.ErrorMessage.text()

                'ns2:TAX_JURI_CODE_NUM' {
                    'ns2:TXJCD'(computeTxjcd(geocode, ctx.exchangejcdunifyind))
                    'ns2:SEQUENCE_NUM'(seqNum)
                    'ns2:MSG_RETURN' {
                        'ns2:RETCODE'(responseCode == '9999' ? '0' : '1')
                        'ns2:ERRCODE'(responseCode == '9999' ? '0000' : '1999')
                        'ns2:ERRMSG'(errorMsg ?: '')
                    }
                }
            }
        }
    }

    return smb.bind(out).toString()
}

/*****************************************************************************************
 *  TXJCD-Ermittlung gem. Regelwerk
 *****************************************************************************************/
private String computeTxjcd(String geocode, String unifyInd)
{
    if (unifyInd != 'X' || !geocode) return geocode ?: ''

    if (geocode.startsWith('ZZ'))
    {
        return 'US' + geocode
    }
    else if (geocode.startsWith('US') && geocode.size() >= 4)
    {
        String code     = geocode.substring(2, 4)          // Zeichen 3-4 (0-basiert)
        String state    = stateMap()[code] ?: code         // Mappingtabelle, fallback = code
        String rest     = geocode.substring(2)             // ab Pos 3
        return "US${state}${rest}-"
    }
    else
    {
        return geocode
    }
}

/*****************************************************************************************
 *  Staatencode-Mapping (Beispieltabelle)
 *  Hinweis: Ohne konkrete Vorgabe wird hier der Code auf sich selbst abgebildet.
 *****************************************************************************************/
private Map stateMap()
{
    // Vollständige US-Bundesstaatenliste nach Bedarf anlegen
    return [
        '01':'AL', '02':'AK', '04':'AZ', '05':'AR', '06':'CA', '08':'CO', '09':'CT',
        '10':'DE', '11':'DC', '12':'FL', '13':'GA', '15':'HI', '16':'ID', '17':'IL',
        '18':'IN', '19':'IA', '20':'KS', '21':'KY', '22':'LA', '23':'ME', '24':'MD',
        '25':'MA', '26':'MI', '27':'MN', '28':'MS', '29':'MO', '30':'MT', '31':'NE',
        '32':'NV', '33':'NH', '34':'NJ', '35':'NM', '36':'NY', '37':'NC', '38':'ND',
        '39':'OH', '40':'OK', '41':'OR', '42':'PA', '44':'RI', '45':'SC', '46':'SD',
        '47':'TN', '48':'TX', '49':'UT', '50':'VT', '51':'VA', '53':'WA', '54':'WV',
        '55':'WI', '56':'WY'
    ]
}

/*****************************************************************************************
 *  Hilfsfunktion: Attachment im MessageLog ablegen
 *****************************************************************************************/
private void addLogAttachment(def messageLog, String name, String content, String mime = 'text/xml')
{
    try
    {
        messageLog?.addAttachmentAsString(name, content ?: '', mime)
    }
    catch (Exception ex)
    {
        // Silent Fail – Logging darf keinen Prozessabbruch provozieren
    }
}

/*****************************************************************************************
 *  Zentrale Fehlerbehandlung
 *****************************************************************************************/
private void handleError(Message message, Exception e, def messageLog)
{
    String body = message.getBody(String) ?: ''
    addLogAttachment(messageLog, 'ErrorPayload', body, 'text/xml')

    def errMsg = "Fehler im Geocode-Integrationsskript: ${e.message}"
    messageLog?.setStringProperty('GroovyError', errMsg)
    throw new RuntimeException(errMsg, e)
}