/*********************************************************************
*  Fixed-Asset-Master-Transformation  –  SAP Cloud Integration
*  Autor:  Senior-Developer Groovy / SAP CI
*********************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets

/* ===================================================================
 *  MAIN
 * ==================================================================*/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {

        /* 1. Header / Property-Initialisierung */
        setHeadersAndProperties(message)

        /* 2. Base64-Decoding */
        String bodyEncoded  = message.getBody(String) ?: ''
        String bodyDecoded  = decodePayload(bodyEncoded)

        /* 3. Parsing & Filter */
        Map<String, List<Map<String,String>>> worksheets = parseWorkbook(bodyDecoded)

        /* 4. Mapping */
        List<Map<String,String>> mappedRows = performMapping(worksheets, messageLog)

        /* 5. XML-Aufbau & Base64-Encoding */
        String outputXml      = buildOutputXml(mappedRows)
        String outputEncoded  = outputXml
                                .getBytes(StandardCharsets.UTF_8)
                                .encodeBase64()
                                .toString()

        message.setBody(outputEncoded)
        return message

    } catch (Exception e){
        handleError(message.getBody(String), e, messageLog)
        // handleError wirft Exception – die folgende Zeile wird nie erreicht,
        // ist jedoch syntaktisch notwendig
        return message
    }
}

/* ===================================================================
 *  FUNKTIONEN
 * ==================================================================*/

/**
 * Liest Header bzw. Properties aus dem Message-Objekt aus
 * und legt bei Bedarf Platzhalter an.
 */
void setHeadersAndProperties(Message message){
    ['requestUser','requestPassword','requestURL'].each{ key ->
        def val = message.getHeader(key, String) ?: message.getProperty(key)
        message.setProperty(key, val ?: 'placeholder')
    }
}

/**
 * Dekodiert eine Base64-Zeichenkette. 
 * Erkennt XML-Text heuristisch anhand von „<“ als erstes Zeichen.
 */
String decodePayload(String input){
    if(!input?.trim()) return ''
    if(input.trim().startsWith('<')) {
        return input
    }
    try{
        return new String(input.decodeBase64(), StandardCharsets.UTF_8)
    }catch(Exception ignored){
        // Bereits dekodiert
        return input
    }
}

/**
 * Parsed das Workbook (Namespaces werden ignoriert) und
 * liefert zu jedem Worksheet eine Liste von Datenzeilen,
 * wobei jede Zeile als Map<Header, Wert> zurückkommt.
 */
Map<String, List<Map<String,String>>> parseWorkbook(String xmlText){
    def parser   = new XmlParser(false,false)   // namespaceAware = false
    def workbook = parser.parseText(xmlText)
    Map<String, List<Map<String,String>>> result = [:]

    workbook.'Worksheet'.each{ ws ->
        String sheetName = (ws.'@ss:Name') ?: (ws.'@Name') ?: 
                           (ws.attributes().find{it.key.toString().endsWith(':Name')}?.value)
        if(!sheetName) return                       // Kein Name ⇒ nächstes Sheet

        def rows    = ws.Table.Row
        if(rows.size() < 2) return                  // Keine Daten ⇒ nächstes Sheet

        /* Header-Zeile */
        List<String> headers = rows[0].Cell.collect{ it.Data.text().trim() }

        /* Daten-Zeilen */
        List<Map<String,String>> dataRows = []
        rows.list().drop(1).each{ r ->
            Map<String,String> rowMap = [:]
            headers.eachWithIndex{ h, idx ->
                rowMap[h] = (idx < r.Cell.size()) ? r.Cell[idx].Data.text().trim() : ''
            }
            // nur aufnehmen, wenn mindestens ein Wert befüllt
            if(rowMap.values().any{ it }) dataRows << rowMap
        }

        result[sheetName] = dataRows
    }
    return result
}

/**
 * Führt die fachlichen Mapping-Regeln aus.
 * Liefert eine Liste von Ergebnis-Zeilen (Map mit Zielfeldern).
 */
List<Map<String,String>> performMapping(Map worksheets, def messageLog){
    List<Map<String,String>> resultRows = []

    /* Default-Set-of-Book */
    Map<String,String> sobByCompany = [:]
    (worksheets['DefaultSetofBook'] ?: []).each{ r ->
        sobByCompany[r['Company_ID']] = r['Default_Set_of_Book_ID']
    }

    /* Fixed-Asset-Master */
    (worksheets['Fixed_Asset_Master'] ?: []).each{ r ->
        String company    = r['Company']
        String sob        = r['Set_of_Books']
        String fixedAsset = r['Fixed_Asset']
        String assetClass = r['Fixed_Asset_Class']

        /* Validierungen gem. Regelwerk */
        if(!sobByCompany.containsKey(company)){
            messageLog?.addAttachmentAsString('ValidationError',
                    "Unbekannte Company_ID: ${company}", 'text/plain')
            throw new RuntimeException("Company_ID ${company} nicht bekannt.")
        }
        if(sobByCompany[company] != sob){
            // Zeile ignorieren
            messageLog?.addAttachmentAsString('IgnoredRow',
                    "Company ${company} – Set_of_Books ${sob} ≠ Default-SOB ⇒ Zeile ignoriert.",
                    'text/plain')
            return
        }

        /* Ziel-Zeile erstellen */
        resultRows << [
                BUKRS    : company,
                ANLN1    : convertMainAssetNumber(fixedAsset),
                INVENTORY: "INV-${fixedAsset}",
                ANLN2    : '0000',
                ANLKL    : assetClass
        ]
    }
    return resultRows
}

/**
 * Wandelt Fixed-Asset in eine 12-stellige Haupt-Anlagennummer um.
 * Existieren keine Ziffern, wird der Originalwert zurückgegeben.
 */
String convertMainAssetNumber(String asset){
    String digits = asset?.replaceAll('\\D','')
    return digits ? digits.padLeft(12,'0') : asset
}

/**
 * Baut das gewünschte Ziel-Workbook (XML) auf.
 */
String buildOutputXml(List<Map<String,String>> rows){
    int expanded = rows.size() + 5                // 5 feste Header-/Leerzeilen
    StringWriter sw = new StringWriter()
    MarkupBuilder xml = new MarkupBuilder(sw)
    xml.setDoubleQuotes(true)

    xml.Workbook(xmlns: "urn:schemas-microsoft-com:office:spreadsheet",
                 "xmlns:o": "urn:schemas-microsoft-com:office:office",
                 "xmlns:x": "urn:schemas-microsoft-com:office:excel",
                 "xmlns:ss": "urn:schemas-microsoft-com:office:spreadsheet") {
        Worksheet("ss:Name": "Fixed_Asset") {
            Table("ss:ExpandedRowCount": expanded) {

                Row(); Row(); Row(); Row()          // leere Zeilen 1-4

                /* Technische Header */
                Row{
                    ['BUKRS','ANLN1','INVENTORY','ANLN2','ANLKL'].each{
                        Cell{ Data("ss:Type":"String", it) }
                    }
                }
                /* Beschreibungszeile */
                Row{
                    ['Company Code','Main Asset Number','Inventory Number',
                     'Asset Subnumber','Asset Class'].each{
                        Cell{ Data("ss:Type":"String", it) }
                    }
                }
                /* Typenzeile */
                Row{
                    (1..5).each{
                        Cell{ Data("ss:Type":"String", "Type: String") }
                    }
                }
                /* Datenzeilen */
                rows.each{ m ->
                    Row("ss:AutoFitHeight":"0"){
                        [m.BUKRS,m.ANLN1,m.INVENTORY,m.ANLN2,m.ANLKL].each{
                            Cell{ Data("ss:Type":"String", it) }
                        }
                    }
                }
            }
        }
    }
    return "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n${sw}"
}

/**
 * Zentrales Error-Handling: Payload als Attachment anhängen
 * und Exception erneut werfen, um das IFlow scheitern zu lassen.
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: '', "text/xml")
    throw new RuntimeException("Fehler im Mapping-Skript: ${e.message}", e)
}