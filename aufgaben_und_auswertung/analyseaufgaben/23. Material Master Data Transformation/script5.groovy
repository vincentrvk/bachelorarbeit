/******************************************************************************
 *  SAP CPI – Material Master Mapping (ByD → S/4 HANA Public Cloud)
 *
 *  Beschreibung:
 *  1.  Liest den eingehenden Base64-kodierten Workbook-Payload
 *  2.  Dekodiert & parst das XML
 *  3.  Führt das fachliche Mapping gem. Vorgabe durch
 *  4.  Erzeugt ein neues Workbook-XML (Worksheet „BasicData“)
 *  5.  Kodiert den Ergebnis-Payload erneut in Base64
 *  6.  Setzt notwendige Header & Properties
 *  7.  Durchgängiges Error-Handling mit Klartext-Log & Payload-Attachment
 ******************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.StreamingMarkupBuilder

// =================================================================================
//  Haupteinstieg – wird von SAP CPI aufgerufen
// =================================================================================
Message processData(Message message) {

    /* ---------------------------------------------------------
     *  Logging-Instanz beschaffen
     * --------------------------------------------------------- */
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* ---------------------------------------------------------
         *  1. Header & Properties setzen
         * --------------------------------------------------------- */
        setHeadersAndProperties(message, messageLog)

        /* ---------------------------------------------------------
         *  2. Payload dekodieren  (Base64 → XML-String)
         * --------------------------------------------------------- */
        String decodedXml = decodePayload(message.getBody(String) ?: '')

        /* ---------------------------------------------------------
         *  3. Business Mapping durchführen
         * --------------------------------------------------------- */
        String targetXml = executeMapping(decodedXml)

        /* ---------------------------------------------------------
         *  4. Ergebnis wieder Base64-kodieren & Body setzen
         * --------------------------------------------------------- */
        String encodedResult = targetXml.bytes.encodeBase64().toString()
        message.setBody(encodedResult)

        return message

    } catch (Exception ex) {
        /* ---------------------------------------------------------
         *  Fehlerbehandlung
         * --------------------------------------------------------- */
        handleError(message.getBody(String) ?: '', ex, messageLog)
        return null     // wird nie erreicht – handleError wirft Exception
    }
}

// =================================================================================
//  Modul 1 – Header & Property Aufbereitung
// =================================================================================
/**
 * Prüft, ob die benötigten Properties / Header bereits existieren
 * und setzt ansonsten einen Platzhalterwert.
 */
void setHeadersAndProperties(Message message, def messageLog) {

    Map<String, String> defaults = [
        requestUser     : 'placeholder',
        requestPassword : 'placeholder',
        requestURL      : 'placeholder'
    ]

    defaults.each { key, defaultValue ->
        // Zuerst Property prüfen …
        def value = message.getProperty(key) as String
        // … falls leer, Header prüfen …
        if (!value) {
            value = message.getHeader(key, String)
        }
        // … und schlussendlich ggf. Platzhalter setzen.
        if (!value) {
            value = defaultValue
        }
        // Einheitlich als Property & Header ablegen
        message.setProperty(key, value)
        message.setHeader(key, value)
    }

    messageLog?.addAttachmentAsString(
            'HeaderPropertyInfo',
            "Properties & Header erfolgreich gesetzt: ${defaults.keySet()}",
            'text/plain'
    )
}

// =================================================================================
//  Modul 2 – Payload Dekodierung
// =================================================================================
/**
 *  Wandelt einen Base64-String in einen XML-String um
 */
String decodePayload(String base64) {
    if (!base64?.trim()) {
        throw new IllegalArgumentException('Leer/kein Payload erhalten – Abbruch.')
    }
    return new String(base64.bytes.decodeBase64(), 'UTF-8')
}

// =================================================================================
//  Modul 3 – Business Mapping
// =================================================================================
/**
 *  Haupt-Mapping­funktion – ruft Teilfunktionen zum Lesen & Schreiben auf
 */
String executeMapping(String sourceXml) {

    // ----------------------------------------------------------
    //  3.1  Quell-Workbook in Datenstruktur überführen
    // ----------------------------------------------------------
    def slurper   = new XmlSlurper().parseText(sourceXml)
    def workSheets = [:]              // Map<WorksheetName , List<Map<SpaltenName , Wert>>>

    slurper.Worksheet.each { ws ->
        String wsName = ws.@'ss:Name'.text()
        def rows = ws.Table.Row
        if (rows.size() < 2) {
            return     // Worksheet ohne Datenzeile ignorieren
        }
        // Header-Zeile
        List<String> headers = rows[0].Cell.collect { it.Data.text().trim() }
        // Datenzeilen (Speicherung als Liste für spätere Erweiterung)
        List<Map<String, String>> dataRows = []

        rows.list().drop(1).each { row ->
            Map<String, String> rowMap = [:]
            headers.eachWithIndex { h, idx ->
                rowMap[h] = row.Cell[idx]?.Data?.text()?.trim() ?: ''
            }
            dataRows << rowMap
        }
        workSheets[wsName] = dataRows
    }

    /* ---------------------------------------------------------
     *  3.2  Ziel­felder gemäß Business Regeln befüllen
     * --------------------------------------------------------- */
    Map general = workSheets['General']?.first()
    if (!general) {
        throw new IllegalStateException('Worksheet "General" nicht gefunden oder leer.')
    }

    String materialId = general['MATERIAL_ID']
    String baseUom    = general['BASE_UOM']

    // --- EAN11 ------------------------------------------------
    String ean11 = ''
    workSheets['GTINs']?.find {
        it['MATERIAL_ID'] == materialId && it['TRADING_UNIT'] == baseUom
    }?.with { matched ->
        ean11 = matched['GTIN'] ?: ''
    }

    // --- UoMCharacteristics ----------------------------------
    Map<String, String> uomMatch = [:]
    workSheets['UoMCharacteristics']?.find {
        it['MATERIAL_ID'] == materialId && it['UNIT_OF_MEASURE'] == baseUom
    }?.with { matched ->
        uomMatch = matched
    }

    String brgew = uomMatch['GROSS_WEIGHT']      ?: ''
    String ntgew = uomMatch['NET_WEIGHT']        ?: ''
    String laeng = uomMatch['LENGTH']            ?: ''
    String gewei = uomMatch['NET_WEIGHT_UOM']    ?: ''

    /* ---------------------------------------------------------
     *  3.3  Ziel-Workbook (Worksheet „BasicData“) aufbauen
     * --------------------------------------------------------- */
    def ns = 'urn:schemas-microsoft-com:office:spreadsheet'
    def builder = new StreamingMarkupBuilder(encoding: 'UTF-8')

    String targetXml = builder.bind {
        mkp.declareNamespace(
                ''  : ns,
                'o' : 'urn:schemas-microsoft-com:office:office',
                'x' : 'urn:schemas-microsoft-com:office:excel',
                'ss': ns
        )

        Workbook {
            Worksheet([('ss:Name'): 'BasicData']) {
                Table([('ss:ExpandedRowCount'): '10']) {

                    // 4 leere Zeilen
                    (1..4).each { Row() }

                    // Kopf­zeile mit Feldnamen
                    Row {
                        ['EAN11', 'BRGEW', 'NTGEW', 'LAENG', 'GEWEI'].each { val ->
                            Cell { Data(['ss:Type': 'String'], val) }
                        }
                    }
                    // Kopf­zeile Beschreibungen
                    Row {
                        ['EAN Code', 'Gross Weight', 'Net Weight', 'Length', 'Weight Unit'].each { val ->
                            Cell { Data(['ss:Type': 'String'], val) }
                        }
                    }
                    // Leer­zeile
                    Row()

                    // Datentyp-Zeile
                    Row {
                        (1..5).each {
                            Cell { Data(['ss:Type': 'String'], 'Type: String') }
                        }
                    }

                    // Daten­zeile
                    Row {
                        [ean11, brgew, ntgew, laeng, gewei].each { val ->
                            Cell { Data(['ss:Type': 'String'], val) }
                        }
                    }
                }
            }
        }
    }.toString()

    return targetXml
}

// =================================================================================
//  Modul 4 – Zentrales Error-Handling
// =================================================================================
/**
 *  Leitet den Payload als Attachment weiter und wirft eine RuntimeException
 */
def handleError(String body, Exception e, def messageLog) {
    // Logging im CPI-Monitoring
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}