/****************************************************************************************
 *  Groovy-Skript – Material Master Mapping (SAP CPI)                                    *
 *  Autor: AI (Senior Integration Developer)                                             *
 *                                                                                      *
 *  Dieses Skript liest einen Base64-kodierten Excel-XML-Payload (SpreadsheetML) ein,    *
 *  führt die geforderten Transformationen durch und liefert wiederum eine              *
 *  Base64-kodierte Excel-XML-Struktur zurück.                                           *
 *                                                                                      *
 *  Modularer Aufbau:                                                                   *
 *  1. setRequestAttributes()    – Properties & Header setzen/auslesen                 *
 *  2. decodePayload()           – Base64-Dekodierung                                   *
 *  3. parseWorksheets()         – Einlesen der drei Arbeitsblätter                     *
 *  4. mapBasicData()            – Umsetzung der Mapping-Regeln                         *
 *  5. buildOutputXml()          – Aufbau des Ziel-Workbooks                            *
 *  6. encodePayload()           – Base64-Kodierung                                     *
 *  7. handleError()             – Einheitliches Error-Handling                         *
 ****************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder

Message processData(Message message) {

    def messageLog = messageLogFactory.getMessageLog(message)
    def originalBody = message.getBody(String) ?: ''

    try {
        // 1. Header / Property Handling
        setRequestAttributes(message, messageLog)

        // 2. Base64-Decode
        String xmlString = decodePayload(originalBody)

        // 3. Worksheets parsen
        Map<String, List<Map<String, String>>> worksheets = parseWorksheets(xmlString)

        // 4. Mapping durchführen
        Map<String, String> mappedValues = mapBasicData(worksheets)

        // 5. Ziel-XML erstellen
        String outXml = buildOutputXml(mappedValues)

        // 6. Base64-Encode und in Message schreiben
        String outB64 = encodePayload(outXml)
        message.setBody(outB64)

        // etwas Logging
        messageLog?.addAttachmentAsString('OutputXML', outXml, 'text/xml')
        return message

    } catch (Exception e) {
        // zentrales Error-Handling
        handleError(originalBody, e, messageLog)
        return message        // wird nie erreicht, da handleError() wirft
    }
}

/* ====================================================================================== */
/*                                 Funktions-Definitionen                                 */
/* ====================================================================================== */

/**
 * Liest Properties / Header aus dem Message-Objekt oder belegt sie mit "placeholder".
 */
void setRequestAttributes(Message msg, def msgLog) {
    ['requestUser', 'requestPassword', 'requestURL'].each { key ->
        def val = msg.getProperty(key) ?: msg.getHeader(key, String) ?: 'placeholder'
        msg.setProperty(key, val)
        msgLog?.setStringProperty("PROP_$key", val)
    }
}

/**
 * Dekodiert einen Base64-String zu XML-Text.
 */
String decodePayload(String base64Body) {
    return new String(base64Body.decodeBase64(), 'UTF-8')
}

/**
 * Liest die relevanten Worksheets und liefert pro Blatt eine Liste aus Zeilen-Maps
 *   Rückgabe-Struktur:  [ 'General':            [ [colName: value, ...] ],
 *                         'GTINs':              [ … ],
 *                         'UoMCharacteristics': [ … ] ]
 */
Map<String, List<Map<String, String>>> parseWorksheets(String xmlString) {

    def ns = new groovy.xml.Namespace('urn:schemas-microsoft-com:office:spreadsheet', 'ss')
    def slurper = new XmlSlurper(false, false)
    slurper.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false)
    def workbook = slurper.parseText(xmlString)

    Map<String, List<Map<String, String>>> result = [:].withDefault { [] }

    workbook.Worksheet.each { ws ->
        String wsName = ws.attribute(ns.Name)?.text()
        if (!wsName) return

        def rows = ws.Table.Row
        if (rows.size() < 2) return                                      // Keine Daten

        // Header-Zeile ermitteln
        List<String> headers = rows[0].Cell.Data*.text()

        // Ab Zeile 2: Datensätze
        rows.drop(1).each { r ->
            List<String> values = r.Cell.Data*.text()
            Map<String, String> rowMap = [:]
            headers.eachWithIndex { h, idx -> rowMap[h] = values.size() > idx ? values[idx] : '' }
            result[wsName] << rowMap
        }
    }
    return result
}

/**
 * Umsetzung der Mapping-Vorgaben für BasicData.
 * Liefert Map mit den Ziel-Feldern: EAN11, BRGEW, NTGEW, LAENG, GEWEI
 */
Map<String, String> mapBasicData(Map<String, List<Map<String, String>>> worksheets) {

    Map<String, String> target = [
            EAN11 : '',
            BRGEW : '',
            NTGEW : '',
            LAENG : '',
            GEWEI : ''
    ]

    if (!worksheets['General']) {
        return target
    }

    Map<String, String> general = worksheets['General'][0]   // laut Vorgabe nur eine Datenzeile
    String matId   = general['MATERIAL_ID']
    String baseUom = general['BASE_UOM']

    /* --------------------------   GTINs   -------------------------- */
    worksheets['GTINs']?.find { it['MATERIAL_ID'] == matId && it['TRADING_UNIT'] == baseUom }?.with {
        target.EAN11 = it['GTIN'] ?: ''
    }

    /* ----------------------   UoMCharacteristics   ----------------- */
    worksheets['UoMCharacteristics']?.find {
        it['MATERIAL_ID'] == matId && it['UNIT_OF_MEASURE'] == baseUom
    }?.with {
        target.BRGEW = it['GROSS_WEIGHT']      ?: ''
        target.NTGEW = it['NET_WEIGHT']        ?: ''
        target.LAENG = it['LENGTH']            ?: ''
        target.GEWEI = it['NET_WEIGHT_UOM']    ?: ''
    }

    return target
}

/**
 * Baut das Zielformat (SpreadsheetML) mit festgelegtem Layout auf.
 */
String buildOutputXml(Map<String, String> v) {

    StringWriter writer = new StringWriter()
    MarkupBuilder mb   = new MarkupBuilder(writer)

    // XML-Deklaration
    writer << '<?xml version="1.0" encoding="utf-8"?>\n'

    mb.Workbook('xmlns':'urn:schemas-microsoft-com:office:spreadsheet',
                'xmlns:o':'urn:schemas-microsoft-com:office:office',
                'xmlns:x':'urn:schemas-microsoft-com:office:excel',
                'xmlns:ss':'urn:schemas-microsoft-com:office:spreadsheet') {

        Worksheet('ss:Name':'BasicData') {
            Table('ss:ExpandedRowCount':'10') {

                // 4 leere Zeilen
                4.times { Row() }

                /* ----- Technische Feldnamen ----- */
                Row {
                    ['EAN11','BRGEW','NTGEW','LAENG','GEWEI'].each {
                        Cell { Data('ss:Type':'String', it) }
                    }
                }

                /* ----- Beschreibungen ----- */
                Row {
                    ['EAN Code','Gross Weight','Net Weight','Length','Weight Unit'].each {
                        Cell { Data('ss:Type':'String', it) }
                    }
                }

                Row()        // Leerzeile

                /* ----- Datentypen ----- */
                Row {
                    5.times { Cell { Data('ss:Type':'String', 'Type: String') } }
                }

                /* ----- Wertezeile ----- */
                Row {
                    [v.EAN11, v.BRGEW, v.NTGEW, v.LAENG, v.GEWEI].each {
                        Cell { Data('ss:Type':'String', it ?: '') }
                    }
                }
            }
        }
    }
    return writer.toString()
}

/**
 * Kodiert den XML-String erneut als Base64.
 */
String encodePayload(String xml) {
    return xml.bytes.encodeBase64().toString()
}

/**
 * Zentrales Error-Handling gemäss Vorgabe. Fügt den fehlerhaften Payload als
 * Attachment hinzu und wirft eine RuntimeException.
 */
def handleError(String body, Exception e, def messageLog) {
    messageLog?.addAttachmentAsString("ErrorPayload", body, "text/xml")
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}