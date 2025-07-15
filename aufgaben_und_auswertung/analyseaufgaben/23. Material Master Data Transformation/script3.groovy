/******************************************************************************
 *  Groovy-Skript – Material Master Data Mapping (ByD -> SAP S/4HANA PCE)
 *  Autor: AI-Assistant – Senior Integration Developer
 *
 *  Beschreibung:
 *  1.  Liest einen Base64-kodierten Workbook-Payload aus der Message.
 *  2.  Dekodiert und parst die Daten in Worksheet-Strukturen.
 *  3.  Ermittelt GTIN, Gewichte, Länge & Gewichtseinheit gem. Mapping-Regeln.
 *  4.  Baut einen neuen Workbook-Payload (Worksheet „BasicData“).
 *  5.  Kodiert das Ergebnis wieder in Base64 und setzt es als Body.
 *  6.  Legt Header/Properties an (place-holder, falls nicht vorhanden).
 *  7.  Durchgängiges Error-Handling mit Attachment des fehlerhaften Payloads.
 ******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder
import java.util.Base64

/* === Entry-Point ========================================================= */
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)          // CPI-Logging

    try {
        setHeadersAndProperties(message, messageLog)                   // Header & Properties

        /* 1. Dekodieren --------------------------------------------------- */
        String decodedXml = decodePayload(message.getBody(String) ?: '')
        if (!decodedXml?.trim()) {
            throw new IllegalStateException('Leerer Payload nach Base64-Dekodierung.')
        }

        /* 2. Parsen & Mapping --------------------------------------------- */
        Map<String, List<List<String>>> workbook = parseWorkbook(decodedXml)
        Map<String, String> mappedValues          = mapMaterialData(workbook)

        /* 3. Workbook für Zielsystem bauen -------------------------------- */
        String outputXml   = buildOutputWorkbook(mappedValues)

        /* 4. Rekodieren & als Body setzen --------------------------------- */
        String encoded     = encodePayload(outputXml)
        message.setBody(encoded)

        messageLog?.addAttachmentAsString('ResultWorkbook', outputXml, 'text/xml')
        return message

    } catch (Exception e) {
        handleError(message.getBody(String) ?: '', e, messageLog)      // delegiert Exception
    }
}

/* === Funktions-Bereich ==================================================== */

/*  Setzt bzw. liest benötigte Header & Properties. */
void setHeadersAndProperties(Message msg, def log) {
    ['requestUser', 'requestPassword', 'requestURL'].each { key ->
        Object val = msg.getProperty(key) ?: msg.getHeader(key, Object) ?: 'placeholder'
        msg.setProperty(key, val)
    }
    log?.addAttachmentAsString('HeaderPropertyInfo',
            "requestUser=${msg.getProperty('requestUser')}\n" +
            "requestURL=${msg.getProperty('requestURL')}", 'text/plain')
}

/*  Dekodiert Base64-Payload zu XML-String. */
String decodePayload(String b64) {
    return new String(Base64.decoder.decode(b64.trim()), 'UTF-8')
}

/*  Kodiert XML-String wieder zu Base64. */
String encodePayload(String xml) {
    return Base64.encoder.encodeToString(xml.getBytes('UTF-8'))
}

/*  Parsed das Workbook in eine Map<WorksheetName, List<Row(List<Cell>)>>   */
Map<String, List<List<String>>> parseWorkbook(String xml) {
    def book = new XmlSlurper().parseText(xml)
    Map<String, List<List<String>>> result = [:]

    book.'Worksheet'.each { ws ->
        String wsName = ws.@'ss:Name'.toString()
        List<List<String>> rows = []
        ws.Table.Row.each { r ->
            List<String> cells = r.Cell.Data.collect { it.text().toString() }
            rows << cells
        }
        result[wsName] = rows
    }
    return result
}

/*  Führt das regelbasierte Mapping aus und liefert die Zielwerte             */
Map<String, String> mapMaterialData(Map<String, List<List<String>>> wb) {
    def generalRows = wb['General']    ?: []
    def gtinRows    = wb['GTINs']      ?: []
    def uomRows     = wb['UoMCharacteristics'] ?: []

    // Header entfernen -> nur erste Datenzeile
    Map general = rowToMap(generalRows, ['MATERIAL_ID','BASE_UOM'])
    Map gtin    = listRowsToMap(gtinRows,
                    ['MATERIAL_ID','TRADING_UNIT','GTIN'],
                    general.MATERIAL_ID, general.BASE_UOM, 'TRADING_UNIT')
    Map uom     = listRowsToMap(uomRows,
                    ['MATERIAL_ID','UNIT_OF_MEASURE','GROSS_WEIGHT','NET_WEIGHT','LENGTH','NET_WEIGHT_UOM'],
                    general.MATERIAL_ID, general.BASE_UOM, 'UNIT_OF_MEASURE')

    return [
        EAN11 : gtin?.GTIN           ?: '',
        BRGEW : uom?.GROSS_WEIGHT    ?: '',
        NTGEW : uom?.NET_WEIGHT      ?: '',
        LAENG : uom?.LENGTH          ?: '',
        GEWEI : uom?.NET_WEIGHT_UOM  ?: ''
    ]
}

/*  Hilfsfunktion: wandelt erste Datenzeile in Map.                           */
Map rowToMap(List<List<String>> rows, List<String> headerRow) {
    if (rows.size() < 2) return [:]
    List<String> header = rows[0]
    List<String> data   = rows[1]
    Map m = [:]
    header.eachWithIndex { h, idx ->
        if (headerRow.contains(h)) {
            m[h] = idx < data.size() ? data[idx] : ''
        }
    }
    return m
}

/*  Sucht in Rows nach passendem MaterialId + UOM und liefert Map.           */
Map listRowsToMap(List<List<String>> rows, List<String> headerRow,
                  String materialId, String baseUom, String uomColumnName) {

    if (rows.size() < 2) return [:]
    List<String> header = rows[0]
    int matIdx  = header.indexOf('MATERIAL_ID')
    int uomIdx  = header.indexOf(uomColumnName)

    for (int i = 1; i < rows.size(); i++) {
        List<String> r = rows[i]
        if (r[matIdx] == materialId && r[uomIdx] == baseUom) {
            // Treffer -> komplette Map zurückgeben
            Map tmp = [:]
            header.eachWithIndex { h, idx -> tmp[h] = idx < r.size() ? r[idx] : '' }
            return tmp
        }
    }
    return [:]
}

/*  Baut den neuen Workbook-XML-String für das Zielsystem.                   */
String buildOutputWorkbook(Map<String, String> values) {
    def writer  = new StringWriter()
    def xml     = new MarkupBuilder(writer)
    xml.setDoubleQuotes(true)

    xml.Workbook(
            xmlns : "urn:schemas-microsoft-com:office:spreadsheet",
            'xmlns:o':"urn:schemas-microsoft-com:office:office",
            'xmlns:x':"urn:schemas-microsoft-com:office:excel",
            'xmlns:ss':"urn:schemas-microsoft-com:office:spreadsheet") {

        Worksheet('ss:Name':'BasicData') {
            Table('ss:ExpandedRowCount':'10') {
                4.times { Row() }                                       // Leere Zeilen

                Row {
                    ['EAN11','BRGEW','NTGEW','LAENG','GEWEI'].each {
                        Cell { Data('ss:Type':'String', it) }
                    }
                }
                Row {
                    ['EAN Code','Gross Weight','Net Weight','Length','Weight Unit'].each {
                        Cell { Data('ss:Type':'String', it) }
                    }
                }
                Row()                                                   // Leerzeile

                Row {
                    5.times { Cell { Data('ss:Type':'String', 'Type: String') } }
                }
                Row {
                    Cell { Data('ss:Type':'String',  values.EAN11) }
                    Cell { Data('ss:Type':'String',  values.BRGEW) }
                    Cell { Data('ss:Type':'String',  values.NTGEW) }
                    Cell { Data('ss:Type':'String',  values.LAENG) }
                    Cell { Data('ss:Type':'String',  values.GEWEI) }
                }
            }
        }
    }
    return writer.toString()
}

/*  Zentrales Error-Handling.                                                */
def handleError(String body, Exception e, def messageLog) {
    // Logging im Monitoring (Name, Inhalt, Typ)
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    String errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}