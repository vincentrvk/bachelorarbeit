/****************************************************************************************
 *  Groovy-Script:  Fixed Asset Master – ByD 2 S/4HANA Transformation
 *  Author:        ChatGPT  (Senior-SW-Developer – CPI)
 *  Description:   – Base64-Decoding of inbound payload
 *                 – XML parsing & mapping according to specification
 *                 – Base64-Encoding of outbound payload
 *                 – Dynamic header / property enrichment
 *                 – Comprehensive error handling & logging
 ****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.*

//===============================================================================
// Entry-Point
//===============================================================================
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        // 1. Header / Property Enrichment ------------------------------------------------
        enrichMetaData(message, messageLog)

        // 2. Base64-Decoding -------------------------------------------------------------
        String decodedXml = decodePayload(message.getBody(String) as String, messageLog)

        // 3. Mapping ---------------------------------------------------------------------
        String mappedXml  = performMapping(decodedXml, messageLog)

        // 4. Base64-Encoding -------------------------------------------------------------
        String encodedXml = mappedXml.bytes.encodeBase64().toString()
        message.setBody(encodedXml)

    } catch (Exception e) {
        handleError(message.getBody(String) as String, e, messageLog)
    }

    return message
}

//===============================================================================
//  Function: enrichMetaData
//  Zweck:    Liest / setzt Properties & Header, falls nicht vorhanden
//===============================================================================
def enrichMetaData(Message message, def messageLog) {
    ['requestUser', 'requestPassword', 'requestURL'].each { key ->
        if (!message.getProperty(key)) {
            message.setProperty(key, 'placeholder')
            messageLog?.addAttachmentAsString("Info_${key}", "Property '${key}' not found – set to placeholder", "text/plain")
        }
        if (!message.getHeader(key, Object)) {
            message.setHeader(key, message.getProperty(key))
        }
    }
}

//===============================================================================
//  Function: decodePayload
//  Zweck:    Dekodiert eingehenden Base64-String zu XML
//===============================================================================
String decodePayload(String encodedBody, def messageLog) {
    try {
        return new String(encodedBody?.trim()?.decodeBase64(), 'UTF-8')
    } catch (Exception e) {
        handleError(encodedBody, new Exception("Base64-Decoding fehlgeschlagen: ${e.message}", e), messageLog)
    }
}

//===============================================================================
//  Function: performMapping
//  Zweck:    Führt das komplette Mapping gemäß Vorgabe durch
//===============================================================================
String performMapping(String srcXml, def messageLog) {
    try {
        // ---------------- Parse Source -------------------------------------------------
        def workbook        = new XmlSlurper().parseText(srcXml)
        def ns              = new groovy.xml.Namespace("urn:schemas-microsoft-com:office:spreadsheet", 'ss')
        def wsFixedAsset    = workbook.Worksheet.find { it."${ns.Name}"?.text() == 'Fixed_Asset_Master' }
        def wsDefaultSet    = workbook.Worksheet.find { it."${ns.Name}"?.text() == 'DefaultSetofBook' }

        if (!wsFixedAsset || !wsDefaultSet) {
            throw new Exception('Erforderliche Worksheets nicht gefunden!')
        }

        // ---------------- Helper – Convert Worksheet to List<Map> ----------------------
        Closure<List<Map>> toListMap = { wsNode ->
            def headers = wsNode.Table.Row[0].Cell.collect { it.Data.text() }
            def rowData = wsNode.Table.Row[1].Cell.collect { it.Data.text() }
            [ headers, rowData ].transpose().collectEntries { it }
        }

        // Default Set-of-Books Map  (Company_ID -> Default_Set_of_Book_ID)
        def defaultMap = [:]
        defaultMap.put(
            wsDefaultSet.Table.Row[1].Cell[0].Data.text(),
            wsDefaultSet.Table.Row[1].Cell[1].Data.text()
        )

        // Fixed Asset Data
        def faRow = toListMap(wsFixedAsset)

        // ---------------- Validation ---------------------------------------------------
        def company        = faRow['Company']
        def setOfBooks     = faRow['Set_of_Books']
        def defaultSob     = defaultMap[company]

        if (!defaultSob) {
            throw new Exception("Company '$company' nicht in DefaultSetofBook vorhanden.")
        }
        if (defaultSob != setOfBooks) {
            messageLog?.addAttachmentAsString(
                    "Info_Skip_Row",
                    "Set_of_Books '$setOfBooks' stimmt nicht mit Default '$defaultSob' überein – Zeile wird ignoriert.",
                    "text/plain")
            return buildEmptyWorkbook()
        }

        // ---------------- Create Target Workbook --------------------------------------
        return buildWorkbook(faRow, company)

    } catch (Exception e) {
        handleError(srcXml, e, messageLog)
    }
}

//===============================================================================
//  Function: buildWorkbook
//  Zweck:    Erzeugt Ziel-Workbook mit Datenzeile
//===============================================================================
String buildWorkbook(Map faRow, String company) {

    // Zielwerte gemäß Mapping / Beispiel
    def BUKRS      = company
    def fixedAsset = faRow['Fixed_Asset']
    def ANLN1      = fixedAsset
    def INVENTORY  = "INV-${fixedAsset}"
    def ANLN2      = fixedAsset        // laut Mapping-Vorgabe
    def ANLKL      = fixedAsset        // laut Mapping-Vorgabe

    def nsAttrs = [
            'xmlns' : 'urn:schemas-microsoft-com:office:spreadsheet',
            'xmlns:o': 'urn:schemas-microsoft-com:office:office',
            'xmlns:x': 'urn:schemas-microsoft-com:office:excel',
            'xmlns:ss': 'urn:schemas-microsoft-com:office:spreadsheet'
    ]

    def writer = new StringWriter()
    def xml    = new MarkupBuilder(writer)

    xml.Workbook(nsAttrs) {
        Worksheet('ss:Name': 'Fixed_Asset') {
            Table('ss:ExpandedRowCount': '11') {
                Row()
                Row()
                Row()
                Row()
                Row {
                    ['BUKRS','ANLN1','INVENTORY','ANLN2','ANLKL'].each { hdr ->
                        Cell { Data('ss:Type':'String', hdr) }
                    }
                }
                Row {
                    ['Company Code','Main Asset Number','Inventory Number','Asset Subnumber','Asset Class'].each { hdr ->
                        Cell { Data('ss:Type':'String', hdr) }
                    }
                }
                Row()
                Row {
                    (1..5).each { Cell { Data('ss:Type':'String', "Type: String") } }
                }
                Row('ss:AutoFitHeight':'0') {
                    [BUKRS, ANLN1, INVENTORY, ANLN2, ANLKL].each { val ->
                        Cell { Data('ss:Type':'String', val) }
                    }
                }
            }
        }
    }
    return XmlUtil.serialize(writer.toString())
}

//===============================================================================
//  Function: buildEmptyWorkbook
//  Zweck:    Gibt leeres Workbook zurück (wird benutzt, falls Zeile ignoriert)
//===============================================================================
String buildEmptyWorkbook() {
    def writer = new StringWriter()
    def xml = new MarkupBuilder(writer)
    xml.Workbook()
    return XmlUtil.serialize(writer.toString())
}

//===============================================================================
//  Function: handleError   (bereitgestellt)
//===============================================================================
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}