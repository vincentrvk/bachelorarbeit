/****************************************************************************************************************
*  Groovy-Skript:   ReplicateB2BContactToCDC.groovy
*  Beschreibung  :  Repliziert BusinessPartner-Änderungen aus MDI in die SAP Customer Data Cloud
*  Autor         :  CPI-Integration (Generiert durch ChatGPT)
*****************************************************************************************************************/
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.nio.charset.StandardCharsets
import java.time.Instant
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.util.Base64

Message processData(Message message) {

    // ---------------------------------------------------------------------
    // Funktionsdefinitionen
    // ---------------------------------------------------------------------

    /**
     * Liest alle benötigten Header / Properties aus dem Message-Objekt
     * Fehlt ein Eintrag, wird er mit dem Platzhalter 'placeholder' befüllt
     */
    def initContext = { Message msg ->
        [
            reqUrl   : (msg.getProperty('requestURL')    ?: 'placeholder'),
            reqUser  : (msg.getProperty('requestUser')   ?: 'placeholder'),
            reqPwd   : (msg.getProperty('requestPassword') ?: 'placeholder'),
            hmacKey  : (msg.getProperty('plainHMACKeyForSignature') ?: 'placeholder'),
            apiKey   : (msg.getProperty('apiKey') ?: 'placeholder')
        ]
    }

    /**
     * Erstellt einen JWT-Token gemäss Vorgabe (Base64URL ohne Padding)
     */
    def createJwtToken = { String apiKey ->
        def base64Url = { byte[] bytes ->
            Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
        }

        def headerJson  = JsonOutput.toJson([alg: 'RS256', typ: 'JWT', kid: apiKey])
        def headerEnc   = base64Url(headerJson.getBytes(StandardCharsets.UTF_8))
        def claimEnc    = base64Url(String.valueOf(Instant.now().epochSecond)
                                     .getBytes(StandardCharsets.UTF_8))
        return "${headerEnc}.${claimEnc}"
    }

    /**
     * Signiert den JWT-Token via HmacSHA256 und liefert das Ergebnis als Base64URL
     */
    def signJwt = { String jwt, String secret ->
        Mac mac = Mac.getInstance("HmacSHA256")
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
        byte[] signature = mac.doFinal(jwt.getBytes(StandardCharsets.UTF_8))
        return Base64.getUrlEncoder().withoutPadding().encodeToString(signature)
    }

    /**
     * Führt einen HTTP-GET auf CDC aus, um ein bestehendes Profil zu lesen.
     * Gibt Map des Profilteils zurück oder null, falls nicht gefunden.
     */
    def getAccount = { String baseUrl, String profileId,
                       String jwt, String sig, String user, String pwd ->
        def conn
        try {
            conn = new URL("${baseUrl}/${profileId}").openConnection()
            conn.setRequestMethod("GET")
            conn.setRequestProperty("Authorization",
                    "Basic " + ("${user}:${pwd}")
                            .getBytes(StandardCharsets.UTF_8).encodeBase64().toString())
            conn.setRequestProperty("JWT-Token", jwt)
            conn.setRequestProperty("CDCSignature", sig)
            def rc = conn.getResponseCode()
            if (rc == 200) {
                def json = new JsonSlurper().parse(conn.getInputStream()) as Map
                return json.profile as Map
            }
            if (rc == 404) {        // Kein Profil vorhanden
                return null
            }
            throw new RuntimeException("GET Account liefert ResponseCode ${rc}")
        } finally {
            conn?.disconnect()
        }
    }

    /**
     * Erstellt eine Map aus dem zu importierenden BusinessPartner
     * (Nur die Felder, die wir aus MDI erhalten)
     */
    def mapIncomingProfile = { BusinessPartner bp ->
        [
            firstName : (bp?.Name?.FirstName?.text() ?: null),
            lastName  : (bp?.Name?.LastName?.text() ?: null),
            city      : (bp?.Address?.CityName?.text() ?: null)
        ].findAll { it.value }          // entfernt Null-Einträge
    }

    /**
     * Vergleicht zwei Profile und liefert nur die Felder zurück,
     * bei denen sich ein Wert geändert hat oder neu ist.
     * Gibt leere Map zurück, wenn keinerlei Änderung vorliegt.
     */
    def diffProfiles = { Map existing, Map incoming ->
        Map changes = [:]
        incoming.each { k, v ->
            if (!existing?.containsKey(k) || existing[k] != v) {
                changes[k] = v
            }
        }
        return changes
    }

    /**
     * Baut den Update-Request-Body als Query-String gemäss Vorgabe
     */
    def buildUpdateBody = { String apiKey, String profileId, Map updatedFields ->
        def profileJson = JsonOutput.toJson(updatedFields)
        return "apiKey=${apiKey}&UID=${profileId}&profile=${URLEncoder.encode(profileJson,'UTF-8')}"
    }

    /**
     * Führt das Update-Profil-POST durch
     */
    def updateProfile = { String baseUrl, String profileId, String body,
                          String jwt, String sig, String user, String pwd ->
        def conn
        try {
            conn = new URL("${baseUrl}/${profileId}").openConnection()
            conn.setRequestMethod("POST")
            conn.doOutput = true
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
            conn.setRequestProperty("Authorization",
                    "Basic " + ("${user}:${pwd}")
                            .getBytes(StandardCharsets.UTF_8).encodeBase64().toString())
            conn.setRequestProperty("JWT-Token", jwt)
            conn.setRequestProperty("CDCSignature", sig)
            conn.outputStream.withWriter { it << body }
            def rc = conn.getResponseCode()
            if (rc != 200 && rc != 201) {
                throw new RuntimeException("Update-Profil fehlgeschlagen, ResponseCode ${rc}")
            }
        } finally {
            conn?.disconnect()
        }
    }

    /**
     * Zentrales Error-Handling. Ergänzt die Meldung um das fehlerhafte Payload
     * als Attachment und wirft eine RuntimeException.
     */
    def handleError = { String body, Exception ex, def messageLog ->
        messageLog?.addAttachmentAsString("ErrorPayload", body ?: '', "text/plain")
        def errMsg = "Fehler in ReplicateB2BContactToCDC: ${ex.message}"
        throw new RuntimeException(errMsg, ex)
    }

    // ---------------------------------------------------------------------
    // Verarbeitung
    // ---------------------------------------------------------------------
    def msgLog = messageLogFactory.getMessageLog(message)
    def originalBody = message.getBody(String)                       // Zur Fehlerdokumentation
    def ctx = initContext(message)

    try {
        // 1. BusinessPartner parsen
        def xml = new XmlSlurper().parseText(originalBody)
        def bp = xml.'**'.find { it.name() == 'BusinessPartner' }    // erwartet genau einen

        if (!bp) {
            throw new RuntimeException("Kein BusinessPartner-Element im Payload gefunden!")
        }

        // 2. ProfileID extrahieren & als Property setzen
        def profileId = bp.Common?.IAMID?.text()
        if (!profileId) {
            throw new RuntimeException("IAMID nicht gefunden – kann Profil nicht identifizieren.")
        }
        message.setProperty("profileID", profileId)

        // 3. JWT generieren
        def jwtToken   = createJwtToken(ctx.apiKey)
        def jwtSig     = signJwt(jwtToken, ctx.hmacKey)

        // 4. Bestehendes Profil lesen
        Map existingProfile = getAccount(ctx.reqUrl, profileId, jwtToken, jwtSig,
                                         ctx.reqUser, ctx.reqPwd)

        // 5. Eingehendes Profil aus XML konstruieren
        Map incomingProfile = mapIncomingProfile(bp)

        // 6. Unterschiede ermitteln
        Map diff = diffProfiles(existingProfile ?: [:], incomingProfile)
        if (diff.isEmpty()) {
            throw new RuntimeException("Keine Änderung zum bestehenden Profil festgestellt.")
        }

        // 7. Update-Body bauen
        String updateBody = buildUpdateBody(ctx.apiKey, profileId, diff)

        // 8. Profil aktualisieren
        updateProfile(ctx.reqUrl, profileId, updateBody, jwtToken, jwtSig,
                      ctx.reqUser, ctx.reqPwd)

        // Ergebnis / Logging
        msgLog?.addAttachmentAsString("UpdatedFields", JsonOutput.prettyPrint(JsonOutput.toJson(diff)),
                                      "application/json")
        message.setBody("Update erfolgreich durchgeführt.")
        return message

    } catch (Exception e) {
        handleError(originalBody, e, msgLog)
        return message      // wird nie erreicht, handleError wirft Exception
    }
}