/* IAMProfileMockExtended.groovy
 * GET mit IAM123456 → JSON-Profil
 * GET mit anderer ID → 404 Not Found
 * POST mit 'apiKey' im Body → Erfolgreich
 * ------------------------------------------------ */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def httpMethod = message.getHeader("CamelHttpMethod", String) ?: ""
    def httpPath   = message.getHeader("CamelHttpPath", String) ?: ""
    def body       = message.getBody(String) ?: ""

    def response = ""
    def contentType = "text/plain"
    def statusCode = 200

    if (httpMethod == "GET") {
        if (httpPath.contains("IAM123456")) {
            response = '''{
  "header": {
    "messageId": "MSG-20250603-001",
    "timestamp": "2025-06-03T10:30:00Z"
  },
  "profile": {
    "firstName": "Max",
    "lastName": "Mustermann",
    "city": "Berlin",
    "email": "max.mustermann@example.com",
    "username": "m.mustermann"
  }
}'''
            contentType = "application/json"
        } else {
            response = ""
            statusCode = 404
        }

    } else if (httpMethod == "POST" && body.contains("apiKey")) {
        response = "Erfolgreich"
        contentType = "text/plain"
    }

    message.setHeader("Content-Type", contentType)
    message.setHeader("CamelHttpResponseCode", statusCode)
    message.setBody(response)
    return message
}
