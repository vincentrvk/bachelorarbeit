/***********************************************************************
* Groovy-Skript – Fixed Asset Master Data Transformation
*
* Aufgabe:
* • Base64-dekodieren des eingehenden Payloads
* • XML-Parsing & Mapping entsprechend der Vorgaben
* • Aufbau des Ziel-Workbooks im Excel-XML-Format
* • Base64-Kodieren des Ergebnisses
* • Pflege von Headern & Properties
* • Modulares Fehler-Handling
*
* Autor: SAP CPI – Senior Integration Developer
************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets
import java.util.Base64

Message processData(Message message) {

    /*--- Initialisierung Logging & Error-Handling --------------------*/
    def messageLog = messageLogFactory.getMessageLog(message)
    try {
        /*--- Schritt 1: Header & Properties sicherstellen -------------*/
        setHeadersAndProperties(message)

        /*--- Schritt 2: Base64-Dekodieren -----------------------------*/
        String decodedXml = decodeBase64(message.getBody(String) ?: '')

        /*--- Schritt 3: XML parsen ------------------------------------*/
        def workbook   = new XmlSlurper().parseText(decodedXml)
        Map mapping    = mapFixedAssetData(workbook, messageLog)   // Enthält bereits validierte Business-Daten

        /*--- Schritt 4: Ziel-Workbook aufbauen ------------------------*/
        String resultXml = buildTargetWorkbook(mapping.values() as List)

        /*--- Schritt 5: Base64-Kodieren & als Body setzen -------------*/
        message.setBody(encodeBase64(resultXml))

        /*--- Rückgabe -------------------------------------------------*/
        return message

    } catch (Exception e) {
        handleError(message.getBody(String) ?: '', e, messageLog)   // Re-throw innerhalb der Methode
        return message                                                // formale Rückgabe – wird nie erreicht
    }
}

/*======================================================================
*  Modul-Funktionen
*=====================================================================*/

/*--------------------------------------------------------------
* Stelle sicher, dass alle benötigten Header & Properties belegt
*-------------------------------------------------------------*/
def setHeadersAndProperties(Message msg) {
    /* NOTE:
     * Bei fehlenden Werten wird laut Anforderung mit „placeholder“
     * gearbeitet. Bereits vorhandene Werte werden NICHT überschrieben.
     */
    def defaults = [
        'requestUser'     : 'placeholder',
        'requestPassword' : 'placeholder',
        'requestURL'      : 'placeholder'
    ]
    defaults.each { k, v ->
        if (msg.getProperty(k) == null) { msg.setProperty(k, v) }
    }
}

/*--------------------------------------------------------------
* Base64-Decoding
*-------------------------------------------------------------*/
String decodeBase64(String base64String) {
    if (!base64String?.trim()) { return '' }
    return new String(Base64.decoder.decode(base64String.bytes), StandardCharsets.UTF_8)
}

/*--------------------------------------------------------------
* Base64-Encoding
*-------------------------------------------------------------*/
String encodeBase64(String plainText) {
    return Base64.encoder.encodeToString(plainText.getBytes(StandardCharsets.UTF_8))
}

/*--------------------------------------------------------------
* Mapping-Logik Fixed Asset
*-------------------------------------------------------------*/
Map<String, Map> mapFixedAssetData(workbook, def messageLog) {

    // Hilfs-Funktionen
    Closure extractTable = { ws ->
        /* Extrahiert eine Zeile aus Worksheet als Liste<String> */
        def rows = ws.Table.Row
        List headerCols = rows[0].Cell.Data*.text()                 // Überschriften
        List dataCols   = rows[1].Cell.Data*.text()                 // Werte
        return [header: headerCols, data: dataCols]
    }

    // 1. DefaultSetOfBook lesen
    def dsWorksheet = workbook.Worksheet.find { it.@'ss:Name' == 'DefaultSetofBook' }
    if (!dsWorksheet) { throw new IllegalStateException('Worksheet "DefaultSetofBook" nicht gefunden!') }
    def dsData      = extractTable(dsWorksheet)
    int idxCompany  = dsData.header.indexOf('Company_ID')
    int idxSetBook  = dsData.header.indexOf('Default_Set_of_Book_ID')
    if (idxCompany  < 0 || idxSetBook < 0) {
        throw new IllegalStateException('Spaltenüberschriften im Worksheet "DefaultSetofBook" unvollständig!')
    }
    Map<String, String> defaultSetMap = [:]
    dsWorksheet.Table.Row.findAll { it != dsWorksheet.Table.Row[0] }.each { row ->
        def cols = row.Cell.Data*.text()
        defaultSetMap[ cols[idxCompany] ] = cols[idxSetBook]
    }

    // 2. Fixed_Asset_Master lesen
    def faWorksheet = workbook.Worksheet.find { it.@'ss:Name' == 'Fixed_Asset_Master' }
    if (!faWorksheet) { throw new IllegalStateException('Worksheet "Fixed_Asset_Master" nicht gefunden!') }
    def faData      = extractTable(faWorksheet)

    // Index-Suche in Fixed_Asset_Master
    int idxFA          = faData.header.indexOf('Fixed_Asset')
    int idxCompanyFA   = faData.header.indexOf('Company')
    int idxSetOfBooks  = faData.header.indexOf('Set_of_Books')
    int idxAssetClass  = faData.header.indexOf('Fixed_Asset_Class')

    if (idxFA < 0 || idxCompanyFA < 0 || idxSetOfBooks < 0 || idxAssetClass < 0) {
        throw new IllegalStateException('Spaltenüberschriften im Worksheet "Fixed_Asset_Master" unvollständig!')
    }

    Map<String, Map> mappedRows = [:]   // key = RunningIndex , value = dataMap

    faWorksheet.Table.Row.findAll { it != faWorksheet.Table.Row[0] }.eachWithIndex { row, i ->
        def cols      = row.Cell.Data*.text()
        def company   = cols[idxCompanyFA]
        def setOfBook = cols[idxSetOfBooks]

        if (!defaultSetMap.containsKey(company)) {
            messageLog?.addAttachmentAsString("InvalidCompany_Row${i}", row.toString(), 'text/plain')
            throw new IllegalStateException("Company '${company}' nicht in DefaultSetOfBook vorhanden!")
        }
        if (defaultSetMap[company] != setOfBook) {
            // Zeile ignorieren – Mapping-Regel
            return
        }

        /* Ziel-Mapping aufbauen */
        def target = [
            BUKRS    : company,
            ANLN1    : padAssetNumber(cols[idxFA]),          // Haupt-Asset-Nummer
            INVENT   : "INV-${cols[idxFA]}",                 // Inventar-Nr.
            ANLN2    : '0000',                               // Sub-Asset-Nummer (Default)
            ANLKL    : cols[idxAssetClass]                   // Asset-Klasse
        ]
        mappedRows["Row${i}".toString()] = target
    }
    return mappedRows
}

/*--------------------------------------------------------------
* Hilfsfunktion – Asset-Nummer links auffüllen (12 Stellen)
*-------------------------------------------------------------*/
String padAssetNumber(String asset) {
    def numeric = asset.replaceAll(/[^0-9]/, '')
    return numeric.padLeft(12, '0')
}

/*--------------------------------------------------------------
* Aufbau des Ziel-Excel-XML Workbooks
*-------------------------------------------------------------*/
String buildTargetWorkbook(List<Map> dataRows) {

    def writer  = new StringWriter()
    def builder = new MarkupBuilder(writer)
    builder.mkp.xmlDeclaration(version: '1.0', encoding: 'utf-8')

    builder.Workbook(xmlns: 'urn:schemas-microsoft-com:office:spreadsheet',
                     'xmlns:o' : 'urn:schemas-microsoft-com:office:office',
                     'xmlns:x' : 'urn:schemas-microsoft-com:office:excel',
                     'xmlns:ss': 'urn:schemas-microsoft-com:office:spreadsheet') {

        Worksheet('ss:Name': 'Fixed_Asset') {
            Table('ss:ExpandedRowCount': (9 + dataRows.size()).toString()) {

                /* Leere Zeilen */
                4.times { Row() }

                /* Technische Überschriften */
                Row {
                    ['BUKRS','ANLN1','INVENTORY','ANLN2','ANLKL'].each {
                        Cell { Data('ss:Type':'String', it) }
                    }
                }

                /* Beschreibung */
                Row {
                    ['Company Code','Main Asset Number','Inventory Number','Asset Subnumber','Asset Class'].each {
                        Cell { Data('ss:Type':'String', it) }
                    }
                }

                Row()  // Leerzeile

                /* Datentyp-Zeile */
                Row {
                    5.times { Cell { Data('ss:Type':'String', 'Type: String') } }
                }

                /* Business-Daten */
                dataRows.each { Map r ->
                    Row('ss:AutoFitHeight':'0') {
                        Cell { Data('ss:Type':'String', r.BUKRS) }
                        Cell { Data('ss:Type':'String', r.ANLN1) }
                        Cell { Data('ss:Type':'String', r.INVENT) }
                        Cell { Data('ss:Type':'String', r.ANLN2) }
                        Cell { Data('ss:Type':'String', r.ANLKL) }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/*--------------------------------------------------------------
* Zentrales Error-Handling
*-------------------------------------------------------------*/
def handleError(String body, Exception e, def messageLog) {
    /* Logging im Monitoring (Name, Inhalt, Typ) */
    messageLog?.addAttachmentAsString("ErrorPayload", body ?: '<<Kein Payload>>', "text/xml")
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}