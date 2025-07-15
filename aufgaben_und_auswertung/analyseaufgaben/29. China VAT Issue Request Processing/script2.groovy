/************************************************************************************************
 * SAP CPI – Groovy-Skript
 * Zweck  : Verarbeitung von VAT-Issue Requests für China Golden Tax
 * Autor  : ChatGPT (Senior-Entwickler Integrationen)
 * Version: 1.0
 ************************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import com.sap.esb.datastore.*
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import groovy.xml.MarkupBuilder

/* =============================================================================
 * Error-Handling
 * ============================================================================= */
 /**
  * Wirft eine RuntimeException und hängt das fehlerhafte Payload als Attachment
  * in die MPL, damit es im Monitoring leicht analysiert werden kann.
  */
def handleError(String body, Exception e, Message message){
    def messageLog = messageLogFactory.getMessageLog(message)
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: "no body", "text/xml")
    def errorMsg = "Fehler im Groovy-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

/* =============================================================================
 * Helferfunktionen für Properties & Header
 * ============================================================================= */
 /**
  * Befüllt alle benötigten Properties (und Header) – existiert der Wert bereits
  * am Message-Objekt wird er übernommen, ansonsten mit 'placeholder' belegt.
  */
def initContextValues(Message message){
    def defaults = [
        requestUser              : 'placeholder',
        requestPassword          : 'placeholder',
        requestURL               : 'placeholder',
        digitalSignEnabled       : 'false',
        plainHMACKeyForSignature : 'placeholder'
    ]

    defaults.each{ k,v ->
        if(!message.getProperty(k)){
            message.setProperty(k, v)
        }
    }
}

/* =============================================================================
 * Datastore Zugriff
 * ============================================================================= */
 /**
  * Liefert die DataStore Instanz aus dem Camel-Context zurück.
  */
def getDataStore(Message message){
    def camelCtx = message.exchange.getContext()
    return camelCtx.getRegistry().lookupByName(DataStore.class.getName()) as DataStore
}

/* =============================================================================
 * Signatur & Kodierung
 * ============================================================================= */
 /**
  * Base64-Kodierung eines Strings (UTF-8).
  */
def toBase64(String input){
    return input?.bytes?.encodeBase64()?.toString()
}

/**
 * HMAC-SHA256 Signatur (Base64 kodiert) über den Eingabetext.
 */
def createHMACSignature(String input, String secret){
    Mac mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"))
    return mac.doFinal(input.getBytes("UTF-8")).encodeBase64().toString()
}

/* =============================================================================
 * HTTP-Aufruf
 * ============================================================================= */
 /**
  * Führt den VAT-Issue POST Call aus und liefert den Response-Body zurück.
  */
def postVatIssue(String base64Payload, String taxNumber, Message message){
    def urlStr   = "${message.getProperty('requestURL')}/${taxNumber}"
    def user     = message.getProperty('requestUser')
    def password = message.getProperty('requestPassword')
    def authHeader = "${user}:${password}".bytes.encodeBase64().toString()

    HttpURLConnection conn = new URL(urlStr).openConnection() as HttpURLConnection
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setRequestProperty("Authorization", "Basic ${authHeader}")
    conn.setRequestProperty("Content-Type", "text/plain")

    // Signatur-Header (optional)
    def signHeader = message.getHeader("signature", String)
    if(signHeader){
        conn.setRequestProperty("signature", signHeader)
    }

    conn.outputStream.withWriter("UTF-8"){ it << base64Payload }

    if(conn.responseCode != 200){
        throw new RuntimeException("HTTP-Fehler: ${conn.responseCode} – ${conn.responseMessage}")
    }

    return conn.inputStream.getText("UTF-8")
}

/* =============================================================================
 * Mapping-Funktionen
 * ============================================================================= */
 /**
  * Erstellt das GoldenTaxDocument_Result Payload & speichert es im Datastore.
  */
def persistGoldenTaxResult(DataStore ds, String goldenTaxNumber){
    def resultWriter = new StringWriter()
    new MarkupBuilder(resultWriter).GoldenTax{
        GoldenTaxNumber(goldenTaxNumber)
        CPIStatus('Q')
        OriginalAction('I')
        ReturnPayload{
            InvoiceHeader{
                OriginalAction('I')
                InvoiceStatus('P')
                GoldenTaxNumber(goldenTaxNumber)
                Messages{
                    ShortMessage('Invoice is pending with query process')
                }
            }
        }
    }

    def data = new Data("GoldenTaxDocument_Result", null, goldenTaxNumber,
                        resultWriter.toString().getBytes("UTF-8"))
    ds.put(data, true, false, 0, 0)
}

/**
 * Erstellt das gewünschte Response-Mapping & legt es im Datastore ab.
 */
def buildAndStoreResponseMapping(Message message, DataStore ds, String taxNo,
                                 List<Map> mappingDetails){
    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)
    xml.GoldenTax{
        Control{
            TaxNumber(taxNo)
            RetryCount('1')
            mappingDetails.each{ det ->
                MappingDetail{
                    GoldenTaxNumber(det.goldenTaxNumber)
                    InvoiceType(det.invoiceType)
                    InvoiceMedium(det.invoiceMedium)
                }
            }
        }
    }

    def data = new Data("QueryRequest_Storage", null, taxNo,
                        writer.toString().getBytes("UTF-8"))
    ds.put(data, true, false, 0, 0)
}

/* =============================================================================
 * Hauptverarbeitung
 * ============================================================================= */
Message processData(Message message){
    try{
        /* ---------- Initialisierung ---------- */
        initContextValues(message)
        def dataStore = getDataStore(message)

        /* ---------- Requests aus Datastore holen ---------- */
        List<Data> requestEntries = dataStore.select("IssueRequest_Storage", 10)
        if(!requestEntries){
            message.setBody("Keine Datastore-Einträge vorhanden.")
            return message
        }

        /* ---------- Verarbeitung je Request ---------- */
        requestEntries.each{ entry ->
            def payload        = new String(entry.getDataAsArray(), "UTF-8")
            def xmlRequest     = new XmlSlurper().parseText(payload)
            def taxNumber      = xmlRequest.'**'.find{ it.name() == 'TaxNumber' }?.text() ?: "UNKNOWN"

            message.setProperty("taxNumber", taxNumber)

            /* --- Filter (Root bleibt <GoldenTax>) ggf. hier anpassbar --- */

            /* --- Kodieren & Signieren --- */
            def base64Payload = toBase64(payload)
            if(message.getProperty('digitalSignEnabled') == 'true'){
                def signature = createHMACSignature(base64Payload, message.getProperty('plainHMACKeyForSignature'))
                message.setHeader("signature", signature)
            }

            /* --- HTTP Call --- */
            def responseBody = postVatIssue(base64Payload, taxNumber, message)

            /* --- Response-Verarbeitung & Persistenz --- */
            def xmlResponse        = new XmlSlurper().parseText(responseBody)
            def mappingDetailList  = []

            xmlResponse.'**'.findAll{ it.name() == 'GTHeader' }.each{ header ->
                def gtn = header.GoldenTaxNumber.text()
                def itp = header.InvoiceType.text()
                def imd = header.InvoiceMedium.text()

                // Property-Sammlung
                mappingDetailList << [goldenTaxNumber: gtn,
                                      invoiceType    : itp,
                                      invoiceMedium  : imd]

                // GoldenTaxDocument_Result Datastore
                persistGoldenTaxResult(dataStore, gtn)
            }

            /* --- Response Mapping erstellen + speichern --- */
            buildAndStoreResponseMapping(message, dataStore, taxNumber, mappingDetailList)

            /* --- Optional: Ursprungs-Eintrag löschen (bereits verarbeitet) --- */
            dataStore.delete("IssueRequest_Storage", entry.getHeaders()?.id ?: entry.id)
        }

        message.setBody("Verarbeitung erfolgreich abgeschlossen.")
        return message

    }catch(Exception e){
        handleError(message.getBody(String) as String, e, message)
    }
}