/***********************************************************************
*  CPI – Groovy‐Script                                                  *
*  Beschreibung:                                                        *
*  – End-to-End Verarbeitung eines CDC-Events (Splitter-tauglich).      *
*  – Aufruf „Get Account“ (CDC) + Mapping + Aufruf „Replicate MDM“.     *
*  – Vollständiges Error-Handling, Logging & Property-Pflege.           *
***********************************************************************/

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.MarkupBuilder
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.net.HttpURLConnection
import java.util.Base64

// ------------------------------------------------------------
// Einstiegspunkt des Skriptes
// ------------------------------------------------------------
Message processData(Message message) {

    /* Ursprünglichen Payload (für spätere Attachments) laden   */
    final String originalBody = message.getBody(String) as String
    def messageLog          = messageLogFactory.getMessageLog(message)

    try {

        /* 1. Header & Properties initialisieren                */
        def config = setInitialProperties(message)

        /* 2. Body parsen und Events verarbeiten                */
        def inJson = new JsonSlurper().parseText(originalBody)
        (inJson?.events ?: []).each { Map event ->

            /* 3. Event-Validierung                              */
            validateEvent(event)

            /* 4. Profil-UID & Account-Typ als Properties setzen */
            String profileID   = event?.data?.uid       ?: ''
            String accountType = event?.data?.accountType ?: ''
            message.setProperty('profileID',   profileID)
            message.setProperty('accountType', accountType)

            /* 5. JWT-Token & Signatur erzeugen                  */
            String jwtToken   = createJwtToken(config.apiKey)
            String cdcSig     = createSignature(jwtToken, config.plainHMACKeyForSignature)

            /* 6. GET Account ausführen                          */
            String getUrl     = "${config.requestURL}/${accountType}/${profileID}"
            def   getResponse = callGetAccount(getUrl, config.requestUser, config.requestPassword,
                                               jwtToken, cdcSig)

            /* 7. Profil aus Response ziehen und validieren      */
            Map profileObj = getResponse?.profile
            validateProfile(profileObj)

            /* 8. Mapping (JSON ➜ XML)                           */
            String xmlPayload = mapProfileToMDM(profileObj, getResponse?.emails)

            /* 9. POST – Replicate Profile to MDM                */
            String mdmUrl = "${config.requestURLMDM}/${profileID}"
            replicateToMDM(mdmUrl, config.requestUserMDM, config.requestPasswordMDM, xmlPayload)

            /* 10. XML als neuen Nachrichten-Body setzen         */
            message.setBody(xmlPayload)
        }

    } catch (Exception e) {
        /* Zentrales Fehler-Handling                            */
        handleError(originalBody, e, messageLog)
    }

    return message
}

/* -----------------------------------------------------------
 * 1) Properties & Header setzen / lesen
 * --------------------------------------------------------- */
def setInitialProperties = { Message msg ->
    /*
     *  Funktion ermittelt vorhandene Properties (sofern im IFlow
     *  bereits gesetzt) oder schreibt „placeholder“, falls nicht
     *  vorhanden. Rückgabe ist eine Map mit allen benötigten Werten.
     */
    def required = [
        'requestUser',
        'requestPassword',
        'requestURL',
        'requestURLMDM',
        'requestUserMDM',
        'requestPasswordMDM',
        'plainHMACKeyForSignature',
        'apiKey'
    ]
    def cfg = [:]
    required.each { key ->
        def val = msg.getProperty(key)
        if (!val) {
            val = 'placeholder'
            msg.setProperty(key, val)
        }
        cfg[key] = val
    }
    return cfg
}

/* -----------------------------------------------------------
 * 2) Event-Validierung
 * --------------------------------------------------------- */
def validateEvent = { Map event ->
    /*
     *  Prüft, ob Event-Typ erlaubt ist.
     */
    def validTypes = ['accountCreated', 'accountUpdated', 'accountRegistered']
    if (!validTypes.contains(event?.type)) {
        throw new RuntimeException("Ungültiger Event-Typ: ${event?.type}")
    }
}

/* -----------------------------------------------------------
 * 3) Profil-Validierung
 * --------------------------------------------------------- */
def validateProfile = { Map profile ->
    /*
     *  Prüft, ob Profil & lastName existieren.
     */
    if (!profile) {
        throw new RuntimeException('Profil-Objekt fehlt in CDC-Antwort.')
    }
    if (!profile.lastName) {
        throw new RuntimeException('lastName fehlt im Profil.')
    }
}

/* -----------------------------------------------------------
 * 4) JWT-Token & Signatur generieren
 * --------------------------------------------------------- */
String createJwtToken(String apiKey) {
    // Header
    def headerJson = JsonOutput.toJson([alg: 'RS256', typ: 'JWT', kid: apiKey])
    String headerEncoded = base64UrlEncode(headerJson)

    // Claim (aktueller UNIX Timestamp in Sekunden)
    def claimJson = JsonOutput.toJson([iat: (System.currentTimeMillis() / 1000L) as long])
    String claimEncoded = base64UrlEncode(claimJson)

    // Finaler Token: header.claim   (ohne Signature!)
    return "${headerEncoded}.${claimEncoded}"
}

String createSignature(String token, String hmacKey) {
    Mac mac = Mac.getInstance('HmacSHA256')
    mac.init(new SecretKeySpec(hmacKey.getBytes('UTF-8'), 'HmacSHA256'))
    byte[] rawHmac = mac.doFinal(token.getBytes('UTF-8'))
    return Base64.getEncoder().encodeToString(rawHmac)
}

private String base64UrlEncode(String value) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes('UTF-8'))
}

/* -----------------------------------------------------------
 * 5) Aufruf „Get Account“ (CDC)
 * --------------------------------------------------------- */
def callGetAccount = { String urlStr, String user, String pwd, String jwt, String sig ->
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod('GET')
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(15000)

    String auth = "${user}:${pwd}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${auth}")
    conn.setRequestProperty('JWT-Token',    jwt)
    conn.setRequestProperty('CDCSignature', sig)

    int rc = conn.responseCode
    String resp = rc in 200..299 ? conn.inputStream.text : conn.errorStream?.text
    if (rc != 200) {
        throw new RuntimeException("GET Account fehlgeschlagen (HTTP ${rc}): ${resp}")
    }
    return new JsonSlurper().parseText(resp)
}

/* -----------------------------------------------------------
 * 6) Mapping: JSON ➜ XML für MDM
 * --------------------------------------------------------- */
String mapProfileToMDM(Map profile, Map emailsMap) {

    /* Mail-Liste aufbereiten (nur verifizierte E-Mails)        */
    def mailList = []
    if (emailsMap?.verified) {
        emailsMap.verified.each { m -> if (m.verified) { mailList << m.email } }
    } else if (profile?.email) {
        mailList << profile.email
    }

    /* Zufällige UUID für MessageHeader                         */
    String uuid = java.util.UUID.randomUUID().toString()

    /* XML mit MarkupBuilder erzeugen                           */
    StringWriter writer = new StringWriter()
    def xml = new MarkupBuilder(writer)
    xml.'bp:BusinessPartnerSUITEBulkReplicateRequest'('xmlns:bp': 'urn:sap-com:document:sap:rfc:functions') {
        BusinessPartnerSUITEReplicateRequestMessage {
            MessageHeader {
                UUID(uuid)
                ID(uuid.replaceAll('-', ''))
            }
            BusinessPartner {
                Common {
                    KeyWordsText(profile.firstName ?: '')
                    AdditionalKeyWordsText(profile.lastName  ?: '')
                    Person {
                        Name {
                            GivenName(profile.firstName ?: '')
                            FamilyName(profile.lastName  ?: '')
                        }
                    }
                }
                AddressIndependentCommInfo {
                    mailList.each { mail ->
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

/* -----------------------------------------------------------
 * 7) POST – Replicate Profile to MDM
 * --------------------------------------------------------- */
def replicateToMDM = { String urlStr, String user, String pwd, String xmlBody ->
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection()
    conn.setRequestMethod('POST')
    conn.doOutput       = true
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(15000)
    String auth = "${user}:${pwd}".bytes.encodeBase64().toString()
    conn.setRequestProperty('Authorization', "Basic ${auth}")
    conn.setRequestProperty('Content-Type',  'application/xml; charset=UTF-8')

    conn.outputStream.withWriter('UTF-8') { it << xmlBody }

    int rc = conn.responseCode
    if (!(rc in 200..299)) {
        String resp = conn.errorStream?.text
        throw new RuntimeException("MDM-Replicate fehlgeschlagen (HTTP ${rc}): ${resp}")
    }
}

/* -----------------------------------------------------------
 * 8) Zentrales Error-Handling
 * --------------------------------------------------------- */
def handleError(String body, Exception e, def messageLog) {
    /* Ursprünglichen Payload als Attachment für Debug zwecke */
    messageLog?.addAttachmentAsString('ErrorPayload', body, 'text/plain')
    String errorMsg = "Fehler im CPI-Groovy-Skript: ${e.message}"
    messageLog?.addAttachmentAsString('Exception', errorMsg, 'text/plain')
    throw new RuntimeException(errorMsg, e)
}