/* SimpleBusinessPartnerMock.groovy
 * Simuliert GET, Create und Activate für Business Partner 1001
 * ------------------------------------------------------------ */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def httpPath = message.getHeader("CamelHttpPath", String) ?: ""
    def httpQuery = message.getHeader("CamelHttpQuery", String) ?: ""
    def httpMethod = message.getHeader("CamelHttpMethod", String) ?: "GET"

    def response = ""
    def contentType = "text/plain"

    // --- GET: Abfrage Business Partner mit ID 1001 ---
    if (httpMethod == "GET") {
        if (httpQuery.contains("1001")) {
            response = '''<BusinessPartner>
    <InternalID>1001</InternalID>
    <Customer>
      <InternalID>CUST9876</InternalID>
    </Customer>
    <Common>
      <NaturalPersonIndicator>true</NaturalPersonIndicator>
      <Person>
        <Name>
          <GivenName>Max</GivenName>
          <FamilyName>Mustermann</FamilyName>
          <AdditionalFamilyName>Muster</AdditionalFamilyName>
        </Name>
      </Person>
      <Organisation>
        <Name>
          <FirstLineName>Beispiel GmbH</FirstLineName>
        </Name>
      </Organisation>
    </Common>
    <Role>
      <RoleCode>ZCUST</RoleCode>
    </Role>
    <AddressInformation>
      <Address>
        <PostalAddress>
          <CountryCode>DE</CountryCode>
        </PostalAddress>
      </Address>
    </AddressInformation>
</BusinessPartner>'''
            contentType = "application/xml"
        } else {
            response = "" // kein Treffer
        }

    // --- POST: Create, Update, Activate (nur Textantworten) ---
    } else if (httpMethod == "POST") {
        switch (httpPath) {
            case "/Create":
                response = "Business Partner created"
                break
            case "/Update":
                response = "Business Partner updated"
                break
            case ~"^/\\d+/Activate$":
                response = "Business Partner activated"
                break
            default:
                response = ""
                break
        }

    } else {
        response = "Unsupported HTTP method"
    }

    message.setHeader("Content-Type", contentType)
    message.setBody(response)
    return message
}
