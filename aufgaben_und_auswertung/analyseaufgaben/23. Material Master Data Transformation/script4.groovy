/****************************************************************************************
 * SAP Cloud Integration – Groovy Script                                                  *
 * ------------------------------------------------------------------------------------- *
 * Aufgabe:  Material Master Data – Mapping „Workbook → BasicData (Worksheet)“           *
 * Autorenrolle: Senior-Entwickler                                                        *
 * ------------------------------------------------------------------------------------- *
 * Wichtige Hinweise:                                                                    *
 *  1.  Das Skript folgt strikt den Modularitäts-, Logging- und Error-Handling-Vorgaben. *
 *  2.  Jeder Funktionsblock ist deutsch kommentiert.                                    *
 *  3.  XML-Verarbeitung ausschließlich über XmlSlurper / MarkupBuilder.                *
 *  4.  Kein unnötiger Import und keine globalen Variablen.                              *
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder

// -----------------------------------------------------------------------------
// Einstiegspunkt der IFlow-Groovy-Script-Ausführung
// -----------------------------------------------------------------------------
Message processData(Message message) {

    // MessageLog für das Monitoring
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /*---------------------------------------------------------------------
         * 1. Header / Properties sicherstellen
         *-------------------------------------------------------------------*/
        setConfigValues(message)

        /*---------------------------------------------------------------------
         * 2. Base64-Decoding des eingehenden Payload
         *-------------------------------------------------------------------*/
        String incomingBase64 = message.getBody(String) ?: ''
        String decodedXml     = decodePayload(incomingBase64)

        /*---------------------------------------------------------------------
         * 3. XML filtern (nur Worksheet-Knoten) und in Datenstrukturen überführen
         *-------------------------------------------------------------------*/
        def parsedWorksheets = parseWorksheets(decodedXml)

        /*---------------------------------------------------------------------
         * 4. Business-Mapping durchführen
         *-------------------------------------------------------------------*/
        List<Map<String, String>> mappedRows = performMapping(parsedWorksheets)

        /*---------------------------------------------------------------------
         * 5. Ziel-XML aufbauen
         *-------------------------------------------------------------------*/
        String targetXml = buildTargetWorkbook(mappedRows)

        /*---------------------------------------------------------------------
         * 6. Base64-Encoding des Ergebnisses und Rückgabe
         *-------------------------------------------------------------------*/
        String encodedTarget = encodePayload(targetXml)
        message.setBody(encodedTarget)

        return message

    } catch (Exception e) {
        // zentrales Error-Handling inkl. Anfügen des Original-Payloads
        handleError(message.getBody(String) as String, e, messageLog)
        return message   // wird aufgrund RuntimeException nie erreicht, aber notwendig für groovy-syntax
    }
}

/*-----------------------------------------------------------------------------
 * Funktionsblock: Konfiguration lesen / setzen
 *---------------------------------------------------------------------------*/
private void setConfigValues(Message msg) {
    // Sicherstellen, dass die 3 Properties vorhanden sind
    ['requestUser', 'requestPassword', 'requestURL'].each { key ->
        def current = msg.getProperty(key) ?: msg.getHeader(key, String)
        if (!current) current = 'placeholder'
        msg.setProperty(key, current)
        // Optional ebenfalls als Header ablegen
        msg.setHeader(key, current)
    }
}

/*-----------------------------------------------------------------------------
 * Funktionsblock: Base64-Decoding
 *---------------------------------------------------------------------------*/
private String decodePayload(String base64String) {
    try {
        return new String(base64String.decodeBase64(), 'UTF-8')
    } catch (Exception e) {
        throw new RuntimeException("Fehler beim Base64-Decoding: ${e.message}", e)
    }
}

/*-----------------------------------------------------------------------------
 * Funktionsblock: Base64-Encoding
 *---------------------------------------------------------------------------*/
private String encodePayload(String plainText) {
    return plainText.getBytes('UTF-8').encodeBase64().toString()
}

/*-----------------------------------------------------------------------------
 * Funktionsblock: Worksheets parsen
 *  Ergebnisstruktur:
 *      [
 *          'General' : [ [colName: value, ...], ... ],
 *          'GTINs'   : [ [colName: value, ...], ... ],
 *          'UoMCharacteristics': [...]
 *      ]
 *---------------------------------------------------------------------------*/
private Map<String, List<Map<String, String>>> parseWorksheets(String xml) {

    def workbook = new XmlSlurper(false, false).parseText(xml)
    Map<String, List<Map<String, String>>> result = [:]

    workbook.Worksheet.each { ws ->
        String wsName = ws.@'ss:Name'.text()
        def rows = ws.Table.Row
        if (!rows) { return }                                      // leere Worksheets ignorieren

        // Kopfzeile (Spaltenbezeichnungen) ermitteln
        List<String> headers = rows[0].Cell.Data.collect { it.text().trim() }

        // Datenzeilen in Liste von Maps umwandeln
        List<Map<String, String>> dataRows = []
        rows.findAll { it != rows[0] }.each { row ->
            Map<String, String> rowMap = [:]
            row.Cell.eachWithIndex { cell, idx ->
                rowMap[ headers[idx] ] = cell.Data.text().trim()
            }
            if (!rowMap.isEmpty()) dataRows << rowMap
        }
        result[wsName] = dataRows
    }
    return result
}

/*-----------------------------------------------------------------------------
 * Funktionsblock: Business-Mapping
 *---------------------------------------------------------------------------*/
private List<Map<String, String>> performMapping(Map<String, List<Map<String, String>>> wsData) {

    List<Map<String, String>> generalList = wsData['General'] ?: []
    List<Map<String, String>> gtinsList   = wsData['GTINs']   ?: []
    List<Map<String, String>> uomList     = wsData['UoMCharacteristics'] ?: []

    List<Map<String, String>> mappedRows = []

    generalList.each { gen ->
        String matId  = gen['MATERIAL_ID']
        String baseU  = gen['BASE_UOM']

        /*---------------- EAN11 ------------------------------------------------*/
        String ean = ''
        def gtinMatch = gtinsList.find { it['MATERIAL_ID'] == matId && it['TRADING_UNIT'] == baseU }
        if (gtinMatch) { ean = gtinMatch['GTIN'] ?: '' }

        /*---------------- UoM Characteristics ----------------------------------*/
        def uomMatch = uomList.find { it['MATERIAL_ID'] == matId && it['UNIT_OF_MEASURE'] == baseU }

        String brgew = uomMatch ? (uomMatch['GROSS_WEIGHT'] ?: '')     : ''
        String ntgew = uomMatch ? (uomMatch['NET_WEIGHT']   ?: '')     : ''
        String laeng = uomMatch ? (uomMatch['LENGTH']       ?: '')     : ''
        String geweI = uomMatch ? (uomMatch['NET_WEIGHT_UOM'] ?: '')   : ''

        mappedRows << [
                'EAN11': ean,
                'BRGEW': brgew,
                'NTGEW': ntgew,
                'LAENG': laeng,
                'GEWEI': geweI
        ]
    }
    return mappedRows
}

/*-----------------------------------------------------------------------------
 * Funktionsblock: Aufbau der Ziel-Workbook-XML
 *---------------------------------------------------------------------------*/
private String buildTargetWorkbook(List<Map<String, String>> dataRows) {

    StringWriter writer = new StringWriter()
    MarkupBuilder mb    = new MarkupBuilder(writer)
    mb.setDoubleQuotes(true)                                             // schöneres XML

    int expandedCount = dataRows.size() + 8   // 4 Leerzeilen + 4 feste Zeilen

    mb.Workbook('xmlns':'urn:schemas-microsoft-com:office:spreadsheet',
                'xmlns:o':'urn:schemas-microsoft-com:office:office',
                'xmlns:x':'urn:schemas-microsoft-com:office:excel',
                'xmlns:ss':'urn:schemas-microsoft-com:office:spreadsheet') {

        Worksheet('ss:Name':'BasicData') {
            Table('ss:ExpandedRowCount': expandedCount) {

                /* 4 Leerzeilen ------------------------------------------------*/
                (1..4).each { Row() }

                /* Header-Zeile (technische Feldnamen) ------------------------*/
                Row {
                    ['EAN11','BRGEW','NTGEW','LAENG','GEWEI'].each { hdr ->
                        Cell { Data('ss:Type':'String') { mkp.yield(hdr) } }
                    }
                }

                /* Beschreibung-Zeile ----------------------------------------*/
                Row {
                    ['EAN Code','Gross Weight','Net Weight','Length','Weight Unit']
                            .each { lbl -> Cell { Data('ss:Type':'String'){ mkp.yield(lbl) } } }
                }

                /* Leere Zeile ------------------------------------------------*/
                Row()

                /* Typ-Zeile --------------------------------------------------*/
                Row {
                    (1..5).each { Cell { Data('ss:Type':'String'){ mkp.yield('Type: String') } } }
                }

                /* Datenzeilen ------------------------------------------------*/
                dataRows.each { rowMap ->
                    Row {
                        ['EAN11','BRGEW','NTGEW','LAENG','GEWEI'].each { key ->
                            Cell { Data('ss:Type':'String'){ mkp.yield(rowMap[key] ?: '') } }
                        }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/*-----------------------------------------------------------------------------
 * Zentrales Error-Handling   (Vorgabe aus Aufgabenstellung)
 *---------------------------------------------------------------------------*/
private void handleError(String body, Exception e, def messageLog) {
    // Eingangs-Payload als Attachment beifügen (Typ „text/xml“)
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: '', "text/xml")
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}