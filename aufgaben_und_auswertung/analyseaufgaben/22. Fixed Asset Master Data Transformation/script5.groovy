/****************************************************************************************
*  Groovy-Skript: Fixed Asset Master Data – Transformation SAP ByD -> SAP S/4 HANA
*  -------------------------------------------------------------------------------------
*  Autor: Senior-Integration-Developer
*  Datum: 2024-06-18
*
*  Beschreibung:
*  1.  Liest einen Base64-kodierten XML-Payload (Excel-XML-Format) aus der Message.
*  2.  Dekodiert den Payload und führt eine Validierung / Transformation nach den
*      vorgegebenen Mappings durch.
*  3.  Baut das Ziel-Workbook (Worksheet „Fixed_Asset“) gem. Zielschema auf.
*  4.  Kodiere das Ergebnis wieder in Base64 und schreibt es in den Message-Body.
*  5.  Setzt erforderliche Header & Properties.
*  6.  Vollständiges Error-Handling inkl. Attachment des fehlerhaften Payloads.
*****************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.xml.MarkupBuilder

/* =========================================================== *
 *  Einstiegspunkt – CPI ruft processData(Message) auf
 * =========================================================== */
Message processData(Message message) {
    def messageLog = messageLogFactory.getMessageLog(message)

    try {
        /* 1. Header / Property Handling */
        setHeadersAndProperties(message, messageLog)

        /* 2. Base64-Decoding des Eingangs-Payloads */
        String decodedXml = decodePayload(message.getBody(String) ?: '')
        if (!decodedXml.trim()) {
            throw new IllegalStateException('Leerer Payload nach Base64-Decoding')
        }

        /* 3. Transformation / Mapping */
        String transformedXml = performMapping(decodedXml, messageLog)

        /* 4. Base64-Encoding des Ergebnis-Payloads */
        String encodedResult = encodePayload(transformedXml)

        /* 5. Ergebnis in Message schreiben */
        message.setBody(encodedResult)

    } catch (Exception e) {
        /* Zentrales Error-Handling */
        handleError(message.getBody(String) ?: '', e, messageLog)
    }

    return message
}

/* =========================================================== *
 *  Funktion: Header & Properties setzen
 * =========================================================== */
void setHeadersAndProperties(Message message, def messageLog) {
    /*  Liefert vorhandene Values oder „placeholder“              */
    String pUser     = message.getProperty('requestUser')     ?: message.getHeader('requestUser', String)     ?: 'placeholder'
    String pPassword = message.getProperty('requestPassword') ?: message.getHeader('requestPassword', String) ?: 'placeholder'
    String pURL      = message.getProperty('requestURL')      ?: message.getHeader('requestURL', String)      ?: 'placeholder'

    /*  Properties wieder setzen, damit sie im Flow verfügbar sind */
    message.setProperty('requestUser',     pUser)
    message.setProperty('requestPassword', pPassword)
    message.setProperty('requestURL',      pURL)

    messageLog?.addAttachmentAsString('INFO_HeaderProperty',
            "requestUser=${pUser}, requestURL=${pURL}", 'text/plain')
}

/* =========================================================== *
 *  Funktion: Payload dekodieren
 * =========================================================== */
String decodePayload(String encodedBody) {
    return new String(encodedBody?.decodeBase64() ?: new byte[0], 'UTF-8')
}

/* =========================================================== *
 *  Funktion: Payload erneut Base64-kodieren
 * =========================================================== */
String encodePayload(String rawString) {
    return rawString?.bytes?.encodeBase64()?.toString()
}

/* =========================================================== *
 *  Funktion: Transformation / Mapping
 * =========================================================== */
String performMapping(String sourceXml, def messageLog) {

    /* --- XML einlesen --------------------------------------------------- */
    def workbook = new XmlSlurper().parseText(sourceXml)
    workbook.declareNamespace(['ss':'urn:schemas-microsoft-com:office:spreadsheet'])

    /* --- Worksheets ermitteln ------------------------------------------- */
    def wsMaster  = workbook.Worksheet.find { it.@'ss:Name'.text() == 'Fixed_Asset_Master' }
    def wsDefault = workbook.Worksheet.find { it.@'ss:Name'.text() == 'DefaultSetofBook'    }

    if (!wsMaster || !wsDefault) {
        throw new IllegalStateException('Erforderliche Worksheets nicht gefunden')
    }

    /* --- DefaultSetOfBook‐Tabelle laden --------------------------------- */
    Map<String,String> defaultSetOfBook = [:]   // Company_ID -> Default_Set_of_Book_ID
    def dsbRows = wsDefault.Table.Row
    if (dsbRows.size() < 2) {                    // Minimal Header + 1 Datenzeile
        throw new IllegalStateException('Worksheet DefaultSetofBook enthält keine Daten')
    }
    dsbRows.tail().each { row ->
        def cells = row.Cell.Data*.text()
        if (cells.size() >= 2) {
            defaultSetOfBook[cells[0]] = cells[1]
        }
    }

    /* --- Master‐Tabelle verarbeiten ------------------------------------- */
    def masterRows = wsMaster.Table.Row
    if (masterRows.size() < 2) {
        throw new IllegalStateException('Worksheet Fixed_Asset_Master enthält keine Daten')
    }

    /* Header‐Zeile einlesen, um Spaltenpositionen dynamisch zu ermitteln */
    List<String> headerCols = masterRows.first().Cell.Data*.text()
    Map<String,Integer> idx = [:]
    headerCols.eachWithIndex { col, i -> idx[col] = i }

    /* Prüfen ob Pflicht-Spalten vorhanden sind */
    ['Fixed_Asset','Fixed_Asset_Class','Company','Set_of_Books'].each {
        if (!idx.containsKey(it)) {
            throw new IllegalStateException("Pflicht-Spalte '${it}' nicht gefunden")
        }
    }

    /* --- Datenzeilen iterieren & validieren ----------------------------- */
    List<Map> assetList = []           // Liste der gültigen Assets

    masterRows.tail().each { row ->
        List<String> cells = row.Cell.Data*.text()
        String company      = cells[idx['Company']]
        String sob          = cells[idx['Set_of_Books']]
        String fixedAsset   = cells[idx['Fixed_Asset']]
        String assetClass   = cells[idx['Fixed_Asset_Class']]

        /* Validierung Company / SOB laut Mapping-Regel */
        if (!defaultSetOfBook.containsKey(company)) {
            messageLog?.addAttachmentAsString('WARNUNG_Unbekannte_Company',
                    "Company ${company} in MasterData nicht in DefaultSetofBook – Zeile wird ignoriert",
                    'text/plain')
            return                                         // Zeile ignorieren
        }
        if (sob != defaultSetOfBook[company]) {
            messageLog?.addAttachmentAsString('WARNUNG_SOB_Nicht_Default',
                    "Company ${company}: Set_of_Books ${sob} != Default ${defaultSetOfBook[company]} – Zeile wird ignoriert",
                    'text/plain')
            return                                         // Zeile ignorieren
        }

        /* Transformationen für Ziel-Felder */
        def map = [:]
        map.BUKRS      = company
        map.ANLN1      = fixedAsset.replaceAll('[^0-9]','').padLeft(12,'0')  // 12-stellig numerisch
        map.INVENTORY  = "INV-${fixedAsset}"
        map.ANLN2      = '0000'
        map.ANLKL      = assetClass

        assetList << map
    }

    if (assetList.isEmpty()) {
        throw new IllegalStateException('Keine gültigen Datensätze nach Validierung vorhanden')
    }

    /* --- Ziel-XML erstellen --------------------------------------------- */
    StringWriter sw = new StringWriter()
    MarkupBuilder mb = new MarkupBuilder(sw)
    mb.mkp.xmlDeclaration(version:'1.0', encoding:'utf-8')

    mb.Workbook(xmlns:'urn:schemas-microsoft-com:office:spreadsheet',
                'xmlns:o':'urn:schemas-microsoft-com:office:office',
                'xmlns:x':'urn:schemas-microsoft-com:office:excel',
                'xmlns:ss':'urn:schemas-microsoft-com:office:spreadsheet') {

        Worksheet('ss:Name':'Fixed_Asset') {
            Table {
                /* 4 leere Zeilen wie im Beispiel */
                4.times { Row() }

                /* Überschriften */
                Row {
                    ['BUKRS','ANLN1','INVENTORY','ANLN2','ANLKL'].each {
                        Cell { Data('ss:Type':'String', it) }
                    }
                }
                /* Beschreibungen */
                Row {
                    ['Company Code','Main Asset Number','Inventory Number',
                     'Asset Subnumber','Asset Class'].each {
                        Cell { Data('ss:Type':'String', it) }
                    }
                }
                /* 1 Leerzeile */
                Row()

                /* Datentyp‐Zeile */
                Row {
                    5.times { Cell { Data('ss:Type':'String', 'Type: String') } }
                }

                /* Datenzeilen dynamisch hinzufügen */
                assetList.each { a ->
                    Row('ss:AutoFitHeight':'0') {
                        Cell { Data('ss:Type':'String', a.BUKRS) }
                        Cell { Data('ss:Type':'String', a.ANLN1) }
                        Cell { Data('ss:Type':'String', a.INVENTORY) }
                        Cell { Data('ss:Type':'String', a.ANLN2) }
                        Cell { Data('ss:Type':'String', a.ANLKL) }
                    }
                }
            }
        }
    }
    return sw.toString()
}

/* =========================================================== *
 *  Funktion: Zentrales Error-Handling
 * =========================================================== */
void handleError(String body, Exception e, def messageLog) {
    /*  Payload als Attachment für Monitoring anhängen
     *  (siehe vorgegebenes Code-Snippet)                        */
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/xml')
    def errorMsg = "Fehler im Mapping-Skript: ${e.message}"
    throw new RuntimeException(errorMsg, e)
}

/* =========================================================== *
 *  Platzhalter-Funktion: Externer API-Call
 *  (gem. Modularitäts-Anforderung – aktuell nicht genutzt)
 * =========================================================== */
def callExternalAPI(String url, String user, String password) {
    // Hier könnte z.B. ein REST-Aufruf implementiert werden
    return null
}