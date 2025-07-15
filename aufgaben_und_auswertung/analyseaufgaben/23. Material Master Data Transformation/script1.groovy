/*****************************************************************************************
*  Skript: Material Master Mapping – SAP Business ByDesign -> SAP S/4HANA Public Cloud   *
*  Autor : (generated)                                                                   *
*                                                                                        *
*  Beschreibung                                                                          *
*  ------------                                                                          *
*  1.  Entgegennahme eines Base64-codierten Excel-XML-Workbooks (Spreadsheet-XML).       *
*  2.  Dekodierung, Parsing und Selektion der relevanten Worksheets.                     *
*  3.  Durchführung des geforderten Mappings gemäß Vorgaben.                            *
*  4.  Aufbau des Zielformats (Workbook mit Worksheet „BasicData“).                      *
*  5.  Encodierung des Ergebnisses nach Base64 und Rückgabe im Message-Body.            *
*                                                                                        *
*  Modularität gemäß Vorgabe:                                                            *
*  – setHeadersAndProperties()                                                           *
*  – decodeBase64Payload() / encodeBase64Payload()                                       *
*  – parseMaterialWorkbook()                                                             *
*  – performMapping()                                                                    *
*  – buildOutputWorkbook()                                                               *
*  – handleError()                                                                       *
******************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.nio.charset.StandardCharsets
import java.util.Base64

/*****************************************************************************************
*  Haupt-Einstiegspunkt                                                                  *
******************************************************************************************/
Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1. Header & Properties auslesen / vorbelegen */
        def config = setHeadersAndProperties(message, messageLog)

        /* 2. Payload dekodieren                                                  */
        String base64Body = message.getBody(String) as String
        String decodedXml = decodeBase64Payload(base64Body)

        messageLog?.addAttachmentAsString('DecodedPayload', decodedXml, 'text/xml')

        /* 3. Parsing & Strukturaufbereitung                                       */
        def materialData = parseMaterialWorkbook(decodedXml)

        /* 4. Mapping                                                              */
        def mappedValues = performMapping(materialData)

        /* 5. Ziel-Workbook erzeugen                                               */
        String outputXml = buildOutputWorkbook(mappedValues)

        messageLog?.addAttachmentAsString('MappedPayload', outputXml, 'text/xml')

        /* 6. Ergebnis encodieren und zurückgeben                                  */
        String encodedResult = encodeBase64Payload(outputXml)
        message.setBody(encodedResult)

        return message

    } catch (Exception e) {
        handleError(message.getBody(String) as String, e, messageLog)
    }
}

/*****************************************************************************************
*  Funktion: Header & Properties setzen                                                  *
*  – Liest vorhandene Werte, setzt ggf. „placeholder“.                                   *
******************************************************************************************/
private Map setHeadersAndProperties(Message message, def messageLog) {

    Map<String, Object> result = [:]

    ['requestUser', 'requestPassword', 'requestURL'].each { key ->
        def value = message.getProperty(key) ?: message.getHeader(key, Object) ?: 'placeholder'
        message.setProperty(key, value)          // Property schreiben/überschreiben
        result[key] = value
    }

    messageLog?.addAttachmentAsString('Config', result.toString(), 'text/plain')
    return result
}

/*****************************************************************************************
*  Funktion: Base64-Decodierung                                                          *
******************************************************************************************/
private String decodeBase64Payload(String base64Payload) {
    byte[] decodedBytes = Base64.decoder.decode(base64Payload.trim())
    return new String(decodedBytes, StandardCharsets.UTF_8)
}

/*****************************************************************************************
*  Funktion: Base64-Encodierung                                                          *
******************************************************************************************/
private String encodeBase64Payload(String plainText) {
    return Base64.encoder.encodeToString(plainText.getBytes(StandardCharsets.UTF_8))
}

/*****************************************************************************************
*  Funktion: Workbook parsen und strukturieren                                           *
*  Liefert eine Map mit drei Listen (general, gtins, uomChars)                           *
******************************************************************************************/
private Map parseMaterialWorkbook(String xml) {

    def ns = new groovy.xml.Namespace('urn:schemas-microsoft-com:office:spreadsheet', 'ss')
    def workbook = new XmlSlurper(false, false).parseText(xml)

    Map<String, List<Map<String, String>>> result = [
            general   : [],
            gtins     : [],
            uomChars  : []
    ]

    workbook.Worksheet.each { ws ->
        String wsName = ws.attribute(ns.Name)?.text()

        if (!wsName) { return }   // kein gültiger Sheet-Name

        // Headerzeile ermitteln
        def headerCells = ws.Table.Row[0]?.Cell
        if (!headerCells) { return }

        List<String> headers = headerCells.collect { it.Data.text() }

        // Alle Datenzeilen verarbeiten (ab Index 1)
        ws.Table.Row.findAll { it != ws.Table.Row[0] }.each { row ->
            Map<String, String> rowMap = [:]
            row.Cell.eachWithIndex { cell, idx ->
                rowMap[headers[idx]] = cell.Data.text()
            }
            switch (wsName) {
                case 'General'           : result.general   << rowMap; break
                case 'GTINs'             : result.gtins     << rowMap; break
                case 'UoMCharacteristics': result.uomChars  << rowMap; break
            }
        }
    }
    return result
}

/*****************************************************************************************
*  Funktion: Fachliches Mapping                                                          *
*  – Liefert die Zielwerte EAN11, BRGEW, NTGEW, LAENG, GEWEI                             *
******************************************************************************************/
private Map performMapping(Map materialData) {

    Map<String, String> target = [
            EAN11: '',
            BRGEW: '',
            NTGEW: '',
            LAENG: '',
            GEWEI: ''
    ]

    if (!materialData.general) { return target }   // Keine Basisdaten -> leer zurück

    // Es wird pro General-Eintrag gemappt; Vorgabe: genau eine Zeile
    def general = materialData.general[0]
    String matId  = general['MATERIAL_ID']
    String baseUoM = general['BASE_UOM']

    /* Mapping EAN11 ********************************************************************/
    materialData.gtins.find {
        it['MATERIAL_ID'] == matId && it['TRADING_UNIT'] == baseUoM
    }?.with { row ->
        target.EAN11 = row['GTIN'] ?: ''
    }

    /* Mapping UoMCharacteristics *******************************************************/
    def uomRow = materialData.uomChars.find {
        it['MATERIAL_ID'] == matId && it['UNIT_OF_MEASURE'] == baseUoM
    }

    if (uomRow) {
        target.BRGEW = uomRow['GROSS_WEIGHT']      ?: ''
        target.NTGEW = uomRow['NET_WEIGHT']        ?: ''
        target.LAENG = uomRow['LENGTH']            ?: ''
        target.GEWEI = uomRow['NET_WEIGHT_UOM']    ?: ''
    }

    return target
}

/*****************************************************************************************
*  Funktion: Ziel-Workbook aufbauen                                                      *
******************************************************************************************/
private String buildOutputWorkbook(Map values) {

    def writer  = new StringWriter()
    def builder = new MarkupBuilder(writer)

    builder.mkp.xmlDeclaration(version: '1.0', encoding: 'utf-8')

    builder.Workbook(
            xmlns               : 'urn:schemas-microsoft-com:office:spreadsheet',
            'xmlns:o'           : 'urn:schemas-microsoft-com:office:office',
            'xmlns:x'           : 'urn:schemas-microsoft-com:office:excel',
            'xmlns:ss'          : 'urn:schemas-microsoft-com:office:spreadsheet') {

        Worksheet('ss:Name': 'BasicData') {
            Table('ss:ExpandedRowCount': '10') {

                /* Vier leere Zeilen ***************************************************/
                4.times { Row() }

                /* Überschrift Code-Zeile *********************************************/
                Row {
                    ['EAN11', 'BRGEW', 'NTGEW', 'LAENG', 'GEWEI'].each {
                        Cell { Data('ss:Type': 'String', it) }
                    }
                }

                /* Überschrift Beschreibung *******************************************/
                Row {
                    ['EAN Code', 'Gross Weight', 'Net Weight', 'Length', 'Weight Unit'].each {
                        Cell { Data('ss:Type': 'String', it) }
                    }
                }

                /* Leerzeile **********************************************************/
                Row()

                /* Typ-Zeile ***********************************************************/
                Row {
                    5.times { Cell { Data('ss:Type': 'String', 'Type: String') } }
                }

                /* Datensatz ***********************************************************/
                Row {
                    [values.EAN11, values.BRGEW, values.NTGEW, values.LAENG, values.GEWEI].each {
                        Cell { Data('ss:Type': 'String', it ?: '') }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/*****************************************************************************************
*  Zentrales Error-Handling                                                              *
*  – Fügt den eingehenden Payload als Attachment hinzu und wirft RuntimeException.       *
******************************************************************************************/
private void handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}