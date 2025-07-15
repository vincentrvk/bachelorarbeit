/* SimpleProductMock.groovy
 * Unterstützt GET, Create, Update und Activate für Produkte
 * --------------------------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def httpPath = message.getHeader("CamelHttpPath", String) ?: ""
    def httpQuery = message.getHeader("CamelHttpQuery", String) ?: ""
    def httpMethod = message.getHeader("CamelHttpMethod", String) ?: "GET"

    def response = ""
    def contentType = "text/plain"

    // --- GET: Produktabfrage per Query ---
    if (httpMethod == "GET") {
        if (httpQuery.contains("PROD12345")) {
            response = '''<Product> 
  <ProductInternalID>PROD12345</ProductInternalID>
  <ProductTypeCode>FG</ProductTypeCode>
  <DeletedIndicator>true</DeletedIndicator>
  <ProductGroupInternalID>GRP789</ProductGroupInternalID>
  <Description>
    <Description>TestDescription</Description>
  </Description>
</Product>'''
            contentType = "application/xml"
        } else {
            response = ""
        }

    // --- POST: Produktoperationen ---
    } else if (httpMethod == "POST") {
        switch (httpPath) {
            case "/Create":
                response = "Product created"
                break
            case "/Update":
                response = "Product updated"
                break
            case ~'^/PROD\\d+/Activate$':
                response = "Product activated"
                break
            default:
                response = ""
                break
        }

    // --- Sonstige Methoden ---
    } else {
        response = "Unsupported HTTP method"
    }

    message.setHeader("Content-Type", contentType)
    message.setBody(response)
    return message
}
