/****************************************************************************************
 *  Groovy-Script für SAP CI – Fixed Asset Master Data Transformation
 *  Autor:        AI Assistant
 *  Version:      1.0
 *  Beschreibung: Dekodiert einen Base64-Payload, validiert & mappt Daten aus
 *                zwei Worksheets (Fixed_Asset_Master & DefaultSetofBook) und
 *                erzeugt das Zielformat „Fixed_Asset“. Anschließend wird das
 *                Ergebnis wieder Base64-kodiert zurückgegeben.
 *                Header & Properties werden – falls nicht vorhanden – mit
 *                „placeholder“ vorbelegt.
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets

/* =========================================================================
 *  MAIN
 * ========================================================================= */
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        /* ----------------------------------------------------------
         *  1. Header / Property Vorbelegung
         * ---------------------------------------------------------- */
        setHeadersAndProperties(message, messageLog)

        /* ----------------------------------------------------------
         *  2. Base64 → XML-String
         * ---------------------------------------------------------- */
        String decodedXml = decodePayload(message.getBody(String) ?: "", messageLog)

        /* ----------------------------------------------------------
         *  3. XML parsen & filtern
         * ---------------------------------------------------------- */
        def worksheets = filterWorksheets(decodedXml)

        /* ----------------------------------------------------------
         *  4. Mapping
         * ---------------------------------------------------------- */
        String targetXml = performMapping(worksheets, messageLog)

        /* ----------------------------------------------------------
         *  5. XML-String → Base64
         * ---------------------------------------------------------- */
        String encodedResult = encodePayload(targetXml)

        /* ----------------------------------------------------------
         *  6. Ergebnis zurückgeben
         * ---------------------------------------------------------- */
        message.setBody(encodedResult)

    } catch (Exception e) {
        handleError(message.getBody(String) ?: "", e, messageLog)
    }
    return message
}

/* =========================================================================
 *  FUNKTION: Header & Properties vorbelegen
 * ========================================================================= */
private void setHeadersAndProperties(Message message, def messageLog) {
    // Properties
    message.setProperty("requestUser",     message.getProperty("requestUser")     ?: "placeholder")
    message.setProperty("requestPassword", message.getProperty("requestPassword") ?: "placeholder")
    message.setProperty("requestURL",      message.getProperty("requestURL")      ?: "placeholder")

    // Header-Beispiele (können je nach IFlow-Design erweitert werden)
    message.setHeader("Content-Type", message.getHeader("Content-Type", String) ?: "application/xml")

    messageLog?.addAttachmentAsString("HeaderPropertyInfo",
            "Headers:  ${message.getHeaders()}\nProperties:${message.getProperties()}",
            "text/plain")
}

/* =========================================================================
 *  FUNKTION: Base64 dekodieren
 * ========================================================================= */
private String decodePayload(String encoded, def messageLog) {
    try {
        byte[] decodedBytes = encoded.decodeBase64()
        String xml = new String(decodedBytes, StandardCharsets.UTF_8)
        messageLog?.addAttachmentAsString("DecodedPayload", xml, "text/xml")
        return xml
    } catch (Exception e) {
        throw new RuntimeException("Fehler bei der Base64-Dekodierung – ungültiges Format?", e)
    }
}

/* =========================================================================
 *  FUNKTION: Base64 kodieren
 * ========================================================================= */
private String encodePayload(String plainXml) {
    return plainXml.getBytes(StandardCharsets.UTF_8).encodeBase64().toString()
}

/* =========================================================================
 *  FUNKTION: Worksheets herausfiltern
 *            Liefert eine Map<String, List<Map<String,String>>>:
 *            sheetName → List mit Zeilen (Header=Value)
 * ========================================================================= */
private Map<String, List<Map<String, String>>> filterWorksheets(String xmlString) {

    def workbook = new XmlSlurper().parseText(xmlString)
    Map<String, List<Map<String, String>>> result = [:]

    workbook.'Worksheet'.each { ws ->
        String sheetName = ws.@'ss:Name'.toString()
        def rows        = ws.Table.Row
        if (!rows) { return }                                       // leeres Sheet ignorieren

        /* Header-Zeile auslesen */
        List<String> headers = rows[0].Cell.Data.collect { it.text() } as List

        /* Datenzeilen verarbeiten */
        List<Map<String, String>> dataRows = []
        rows.list().drop(1).each { row ->
            Map<String, String> rowMap = [:]
            row.Cell.eachWithIndex { c, idx ->
                rowMap[ headers[idx] ] = c.Data.text()
            }
            dataRows << rowMap
        }
        result[ sheetName ] = dataRows
    }
    return result
}

/* =========================================================================
 *  FUNKTION: Mapping Fixed Asset
 * ========================================================================= */
private String performMapping(Map<String, List<Map<String, String>>> sheets, def messageLog) {

    /* ----------------------------------------------------------
     *  Input-Sheets holen oder Fehler werfen
     * ---------------------------------------------------------- */
    def defaultSobList = sheets['DefaultSetofBook']
    def masterList     = sheets['Fixed_Asset_Master']

    if (!defaultSobList || !masterList) {
        throw new RuntimeException("Benötigte Worksheets „Fixed_Asset_Master“ oder „DefaultSetofBook“ fehlen.")
    }

    /* Liste Company → SetOfBook aufbauen */
    Map<String, String> companySOB = defaultSobList.collectEntries { [ (it.Company_ID): it.Default_Set_of_Book_ID ] }

    /* ----------------------------------------------------------
     *  Ausgabe-Daten vorbereiten
     * ---------------------------------------------------------- */
    List<Map<String, String>> outRows = []

    masterList.each { row ->
        String company = row.Company
        String sob     = row.Set_of_Books
        String fixed   = row.Fixed_Asset
        String fclass  = row.Fixed_Asset_Class

        /* -- Regel-Validierung -------------------------------- */
        if (!companySOB.containsKey(company)) {
            throw new RuntimeException("Company_ID ${company} ist nicht in DefaultSetofBook vorhanden.")
        }
        if (companySOB[company] != sob) {
            // Zeile ignorieren (lt. Vorgabe kein Output)
            return
        }

        /* -- Transformation ----------------------------------- */
        Map<String, String> tgt = [:]
        tgt.BUKRS     = company
        tgt.ANLN1     = fixed?.replaceAll('\\D','').padLeft(12,'0')          // numerisch auffüllen
        tgt.INVENTORY = "INV-${fixed}"
        tgt.ANLN2     = "0000"
        tgt.ANLKL     = fclass

        outRows << tgt
    }

    if (outRows.isEmpty()) {
        throw new RuntimeException("Keine gültigen Datensätze nach Validierung verbleibend.")
    }

    /* ----------------------------------------------------------
     *  Output-XML erzeugen
     * ---------------------------------------------------------- */
    StringWriter sw = new StringWriter()
    MarkupBuilder mb = new MarkupBuilder(sw)
    mb.setDoubleQuotes(true)

    mb.Workbook(
            'xmlns':'urn:schemas-microsoft-com:office:spreadsheet',
            'xmlns:o':'urn:schemas-microsoft-com:office:office',
            'xmlns:x':'urn:schemas-microsoft-com:office:excel',
            'xmlns:ss':'urn:schemas-microsoft-com:office:spreadsheet'
    ) {
        Worksheet('ss:Name':'Fixed_Asset') {
            Table('ss:ExpandedRowCount': (outRows.size() + 8)) {

                /* 4 leere Zeilen                                                   */
                4.times { Row() }

                /* Kopfzeile (technische Feldnamen)                                 */
                Row {
                    ['BUKRS','ANLN1','INVENTORY','ANLN2','ANLKL'].each { val ->
                        Cell { Data('ss:Type':'String', val) }
                    }
                }

                /* Beschreibungszeile                                               */
                Row {
                    ['Company Code','Main Asset Number','Inventory Number','Asset Subnumber','Asset Class'].each { val ->
                        Cell { Data('ss:Type':'String', val) }
                    }
                }

                /* Eine leere Zeile                                                 */
                Row()

                /* Typzeile                                                         */
                Row {
                    5.times { Cell { Data('ss:Type':'String', 'Type: String') } }
                }

                /* Datenzeilen                                                      */
                outRows.each { m ->
                    Row('ss:AutoFitHeight':'0') {
                        Cell { Data('ss:Type':'String', m.BUKRS)     }
                        Cell { Data('ss:Type':'String', m.ANLN1)     }
                        Cell { Data('ss:Type':'String', m.INVENTORY) }
                        Cell { Data('ss:Type':'String', m.ANLN2)     }
                        Cell { Data('ss:Type':'String', m.ANLKL)     }
                    }
                }
            }
        }
    }

    String xmlResult = "<?xml version='1.0' encoding='utf-8'?>\n${sw.toString()}"
    messageLog?.addAttachmentAsString("MappedPayload", xmlResult, "text/xml")
    return xmlResult
}

/* =========================================================================
 *  FUNKTION: (Platzhalter) API-Call
 *            Kann bei Bedarf für S/4HANA oder ByD Aufrufe genutzt werden
 * ========================================================================= */
private String callApi(String url, String user, String pwd, String payload) {
    // Diese Funktion ist als Platzhalter hinterlegt, um die Anforderung
    // „separate Funktion für jeden API-Call“ zu erfüllen. Die tatsächliche
    // Implementierung erfolgt – je nach IFlow-Design – in einer separaten
    // Groovy-Step oder einem Request-Reply-Knoten.
    return "NotImplemented"
}

/* =========================================================================
 *  FUNKTION: Zentrales Error-Handling
 * ========================================================================= */
private void handleError(String body, Exception e, def messageLog) {
    // Logging im Monitoring
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}