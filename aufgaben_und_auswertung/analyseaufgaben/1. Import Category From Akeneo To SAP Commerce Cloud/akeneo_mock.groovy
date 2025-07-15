/* Akeneo_Mock.groovy
 * Antwortet wahlweise mit Token‑ oder Category‑JSON
 * ------------------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.*

Message processData(Message message) {

    // Request‑Body als JSON einlesen
    def req  = new JsonSlurper().parseText(message.getBody(String) ?: '{}')
    def query = req.query ?: ''

    /* -------- Dummy‑Antwort aufbauen ------------------------------------ */
    Map resp
    if (query =~ /token\(/) {                         // Auth‑Query
        resp = [
            data: [
                token: [
                    data: [ accessToken: 'dummy-token-1234' ]
                ]
            ]
        ]
    } else if (query =~ /categories\(/) {             // Kategorien‑Query
        // Lies ggf. den gesuchten Code aus der Query heraus
        def matcher = query =~ /codes:\s*"(.*?)"/
        String catCode = matcher ? matcher[0][1] : 'demo_code'
        resp = [
            data: [
                categories: [
                    items: [[ code: catCode ]]
                ]
            ]
        ]
    } else {                                         // Unbekannt – minimaler Erfolg
        resp = [ data: [:] ]
    }

    /* -------- Response setzen ------------------------------------------- */
    message.setHeader('Content-Type', 'application/json')
    message.setBody(JsonOutput.toJson(resp))

    return message
}
