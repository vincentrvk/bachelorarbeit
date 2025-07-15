/* LisaMuellerProfileMock.groovy
 * Gibt bei GET /<irgendwas>/IAM123456 den Lisa-Müller-Body zurück
 * --------------------------------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def httpMethod = message.getHeader("CamelHttpMethod", String) ?: ""
    def httpPath   = message.getHeader("CamelHttpPath", String) ?: ""

    def response = ""
    def contentType = "application/json"
    def statusCode = 200

    if (httpMethod == "GET" && httpPath.contains("IAM123456")) {
        response = '''{
  "header": {
    "messageId": "MSG-20250603-001",
    "timestamp": "2025-06-03T10:30:00Z"
  },
  "profile": {
    "firstName": "Lisa",
    "lastName": "Müller",
    "city": "München",
    "email": "lisa.mueller@example.com",
    "username": "l.mueller"
  },
  "emails": {
    "verified": [
      {
        "email": "lisa.mueller@example.com",
        "verified": true,
        "primary": true
      }
    ]
  }
}'''
    } else {
        statusCode = 404
        response = ""
    }

    message.setHeader("Content-Type", contentType)
    message.setHeader("CamelHttpResponseCode", statusCode)
    message.setBody(response)
    return message
}
