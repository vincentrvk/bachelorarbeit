/* ProductAndPlantHandlerMock.groovy
 * Antwortet je nach CamelHttpPath, CamelHttpQuery und CamelHttpMethod
 * ------------------------------------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def httpPath = message.getHeader("CamelHttpPath", String) ?: ""
    def httpQuery = message.getHeader("CamelHttpQuery", String) ?: ""
    def httpMethod = message.getHeader("CamelHttpMethod", String) ?: "GET"

    def response = ""
    def contentType = "text/plain"

    // --- QUERY-Aufrufe (GET) ---
    if (httpMethod == "GET") {
        if (httpQuery.contains("PROD12345")) {
            response = '''<Product>
    <ProductInternalID>PROD12345</ProductInternalID>
    <ProductTypeCode>FG</ProductTypeCode>
    <DeletedIndicator>true</DeletedIndicator>
    <ProductGroupInternalID>GRP789</ProductGroupInternalID>
    <Plant actionCode="01">
        <PlantID>PLANT001</PlantID>
    </Plant>
</Product>'''
            contentType = "application/xml"
        } else if (httpQuery.contains("PLANT001")) {
            response = '''<Plant actionCode="01">
    <PlantID>PLANT001</PlantID>
</Plant>'''
            contentType = "application/xml"
        } else {
            response = "" // Leerer Body bei unbekannter Query
        }

    // --- POST-basierte Operationen ---
    } else if (httpMethod == "POST") {
        switch (httpPath) {
            case "/Create":
                response = "Product created"
                break
            case "/Update":
                response = "Product updated"
                break
            case ~"^/PROD\\d+/Activate$":
                response = "Product activated"
                break
            case "/CreatePlant":
                response = "Plant created"
                break
            case "/UpdatePlant":
                response = "Plant updated"
                break
            default:
                response = "" // Leerer Body bei unbekanntem Pfad
                break
        }

    // --- Ungültige Kombination von Methode + Pfad ---
    } else {
        response = "Unsupported HTTP method"
    }

    message.setHeader("Content-Type", contentType)
    message.setBody(response)
    return message
}
