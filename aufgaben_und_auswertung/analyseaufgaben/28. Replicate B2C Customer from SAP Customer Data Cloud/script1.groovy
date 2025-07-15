/***********************************************************************************************
 * Groovy-Skript:  CDC → MDM – Account Replication
 * Autor        :  Senior-Integration Developer
 * Beschreibung :  Dieses Skript wird in SAP Cloud Integration in einem Groovy-Script-Step
 *                 ausgeführt und übernimmt folgende Aufgaben:
 *                 1. Einlesen der Properties & Header
 *                 2. Event- und Profil-Validierung
 *                 3. JWT-Erstellung und Signatur
 *                 4. Abruf des Profils (GET Account)
 *                 5. Mapping des Profils in das MDM-XML-Format
 *                 6. Durchgängiges Error-Handling mit Payload-Attachment
 ***********************************************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets

/* =======================================
 * Haupteinstieg
 * ===================================== */
Message processData(Message message) {

    // Original-Body sichern (wird im Fehlerfall als Attachment weitergeleitet)
    String originalBody = (message.getBody(String) ?: '')
    def messageLog     = messageLogFactory?.getMessageLog(message)

    try {

        /* 1. Properties & Header vorbereiten */
        prepareHeadersAndProperties(message)

        /* 2. Event-Validierung */
        def event      = extractEvent(originalBody)
        validateEvent(event)

        /* 3. Profil UID & AccountType merken */
        message.setProperty('profileID'   , event.data.uid       )
        message.setProperty('accountType' , event.data.accountType)

        /* 4. JWT-Token erstellen & signieren */
        String jwt      = createJWT(message)
        String signature= signJWT(jwt, message.getProperty('plainHMACKeyForSignature') as String)

        /* 5. Profil aus CDC abrufen */
        Map profileResponse = callGetAccount(message, jwt, signature)

        /* 6. Profil-Validierung */
        validateProfile(profileResponse.profile)

        /* 7. Mapping → MDM-XML */
        String xmlBody = mapToMDMXML(profileResponse)
        message.setBody(xmlBody)

    } catch(Exception e) {
        handleError(originalBody, e, messageLog)   // wirft RuntimeException weiter
    }

    return message
}

/* =======================================
 * Funktion: Properties & Header setzen
 * ===================================== */
void prepareHeadersAndProperties(Message msg) {

    /* Hilfsclosure – liefert vorhandenen Property-Wert oder “placeholder” */
    def getOrDefault = { String key -> msg.getProperty(key) ?: 'placeholder' }

    /* Pflicht-Properties sicherstellen                             */
    ['requestUser'              ,
     'requestPassword'          ,
     'requestURL'               ,
     'requestURLMDM'            ,
     'requestUserMDM'           ,
     'requestPasswordMDM'       ,
     'plainHMACKeyForSignature' ,
     'apiKey'
    ].each { p -> msg.setProperty(p, getOrDefault(p)) }

    /* Beispiel-Header (können bei Bedarf ergänzt werden) */
    if(!msg.getHeader('Content-Type', String)) {
        msg.setHeader('Content-Type', 'application/json')
    }
}

/* =======================================
 * Funktion: Event aus Request-Body holen
 * ===================================== */
Map extractEvent(String body) {
    def json = new JsonSlurper().parseText(body ?: '{}')
    return (json?.events && json.events instanceof List) ? json.events[0] : json
}

/* =======================================
 * Funktion: Event-Validierung
 * ===================================== */
void validateEvent(Map event) {

    if(!event) {
        throw new IllegalArgumentException('Event-Objekt fehlt oder ist leer.')
    }

    List<String> allowedTypes = ['accountCreated','accountUpdated','accountRegistered']
    if(!allowedTypes.contains(event.type)) {
        throw new IllegalArgumentException("Ungültiger Event-Typ: ${event.type}")
    }

    if(!event?.data?.uid) {
        throw new IllegalArgumentException('UID fehlt im Event.')
    }
}

/* =======================================
 * Funktion: Profil-Validierung
 * ===================================== */
void validateProfile(Map profile) {
    if(!profile) {
        throw new IllegalArgumentException('Profil-Objekt fehlt in der Antwort.')
    }
    if(!profile?.lastName) {
        throw new IllegalArgumentException('Profil-Feld lastName ist obligatorisch.')
    }
}

/* =======================================
 * Funktion: JWT erzeugen
 * ===================================== */
String createJWT(Message msg) {

    /* Base64-URL-Encoding ohne Padding */
    def b64Url = { String txt ->
        return txt.bytes.encodeBase64().toString()
                  .replace('+','-').replace('/','_').replace('=','')
    }

    /* Header & Claim erstellen */
    def headerJson = JsonOutput.toJson([alg:'RS256',typ:'JWT',kid:msg.getProperty('apiKey')])
    long ts        = (System.currentTimeMillis() / 1000L).longValue()
    def claimJson  = JsonOutput.toJson([iat:ts])

    return "${b64Url(headerJson)}.${b64Url(claimJson)}"
}

/* =======================================
 * Funktion: JWT signieren (HMAC-SHA256)
 * ===================================== */
String signJWT(String jwt, String plainKey) {

    if(!plainKey || plainKey == 'placeholder') {
        throw new IllegalArgumentException('HMAC-Key fehlt für die Signatur.')
    }

    Mac mac = Mac.getInstance('HmacSHA256')
    mac.init(new SecretKeySpec(plainKey.bytes, 'HmacSHA256'))
    byte[] sigBytes = mac.doFinal(jwt.bytes)

    return sigBytes.encodeBase64().toString()
                   .replace('+','-').replace('/','_').replace('=','')
}

/* =======================================
 * Funktion: GET Account bei CDC
 * ===================================== */
Map callGetAccount(Message msg, String jwt, String signature) {

    String user     = msg.getProperty('requestUser')
    String password = msg.getProperty('requestPassword')
    String urlTxt   = "${msg.getProperty('requestURL')}/${msg.getProperty('accountType')}/${msg.getProperty('profileID')}"

    URLConnection conn = new URL(urlTxt).openConnection()
    conn.setRequestMethod('GET')
    conn.setRequestProperty('Authorization', 'Basic ' + "${user}:${password}".bytes.encodeBase64().toString())
    conn.setRequestProperty('JWT-Token'   , jwt)
    conn.setRequestProperty('CDCSignature', signature)

    int rc = conn.responseCode
    if(rc != 200) {
        throw new RuntimeException("GET Account fehlgeschlagen – HTTP ${rc}")
    }

    String responseBody = conn.inputStream.getText(StandardCharsets.UTF_8.name())
    return new JsonSlurper().parseText(responseBody) as Map
}

/* =======================================
 * Funktion: Mapping → MDM-XML
 * ===================================== */
String mapToMDMXML(Map responseJson) {

    Map profile = responseJson.profile ?: [:]

    String firstName = profile.firstName ?: ''
    String lastName  = profile.lastName  ?: ''
    List  emailList  = (responseJson?.emails?.verified ?: []).collect { it.email }.findAll{ it }

    if(emailList.isEmpty() && profile.email) {
        emailList << profile.email
    }

    String uuid = UUID.randomUUID().toString()

    StringWriter writer = new StringWriter()
    new MarkupBuilder(writer).'bp:BusinessPartnerSUITEBulkReplicateRequest'(
            'xmlns:bp':'urn:sap-com:document:sap:rfc:functions') {

        BusinessPartnerSUITEReplicateRequestMessage {
            MessageHeader {
                UUID(uuid)
                ID(uuid.replaceAll('-',''))
            }
            BusinessPartner {
                Common {
                    KeyWordsText(firstName)
                    AdditionalKeyWordsText(lastName)
                    Person {
                        Name {
                            GivenName(firstName)
                            FamilyName(lastName)
                        }
                    }
                }
                AddressIndependentCommInfo {
                    emailList.each { String mail ->
                        Email {
                            URI(mail)
                        }
                    }
                }
            }
        }
    }

    return writer.toString()
}

/* =======================================
 * Funktion: Error-Handling
 * ===================================== */
void handleError(String body, Exception e, def messageLog) {

    // Payload als Attachment anhängen
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/plain')

    // Aussagekräftige Fehlermeldung werfen
    throw new RuntimeException("CDC→MDM Groovy-Skript fehlgeschlagen: ${e.message}", e)
}