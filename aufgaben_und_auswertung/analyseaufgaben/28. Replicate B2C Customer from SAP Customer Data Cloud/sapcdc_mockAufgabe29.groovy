/* RoleBasedAccountProfileMock.groovy
 * Gibt Profil abhängig von accountType (customer oder prospect) zurück
 * Nur bei ID = IAM123456
 * -------------------------------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def httpMethod = message.getHeader("CamelHttpMethod", String) ?: ""
    def httpPath   = message.getHeader("CamelHttpPath", String) ?: ""

    def response = ""
    def contentType = "application/json"
    def statusCode = 200

    if (httpMethod == "GET") {
        def pathParts = httpPath.tokenize('/')
        def role = pathParts.size() > 0 ? pathParts[0].toLowerCase() : ""
        def id   = pathParts.size() > 1 ? pathParts[1] : ""

        if (id == "IAM123456") {
            if (role == "customer") {
                response = '''{
  "header":{
    "messageId":"MSG-20250603-001",
    "timestamp":"2025-06-03T10:30:00Z"
  },
  "profile":{
    "firstName":"Max",
    "lastName":"Mustermann",
    "city":"Berlin",
    "email":"max.mustermann@example.com",
    "username":"m.mustermann"
  },
  "emails": {
    "verified": [
      {
        "email": "max.mustermann@example.com",
        "verified": true,
        "primary": true
      }
    ]
  }
}'''
            } else if (role == "prospect") {
                response = '''{
  "header": {
    "messageId": "MSG-20250603-001",
    "timestamp": "2025-06-03T10:30:00Z"
  },
  "results": [
    {
      "profile": {
        "firstName": "Max",
        "lastName": "Mustermann",
        "city": "Berlin",
        "email": "max.mustermann@example.com",
        "username": "m.mustermann"
      }
    },
    {
      "opportunityInformation": {
        "projectType": "IT-Modernisierung",
        "estimatedBudgetEUR": 250000,
        "expectedStartDate": "2025-09-01"
      }
    },
    {
      "emails": {
        "verified": [
          {
            "email": "max.mustermann@example.com",
            "verified": true,
            "primary": true
          }
        ]
      }
    }
  ]
}'''
            } else {
                statusCode = 404
                response = ""
            }
        } else {
            statusCode = 404
            response = ""
        }
    } else {
        statusCode = 405
        response = "Unsupported HTTP method"
        contentType = "text/plain"
    }

    message.setHeader("Content-Type", contentType)
    message.setHeader("CamelHttpResponseCode", statusCode)
    message.setBody(response)
    return message
}
