/* BusinessPartnerAndAddressHandlerMock.groovy
 * Antwortet dynamisch auf Business Partner und Adressen
 * anhand von Path, Query, Method
 * ------------------------------------------------------ */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def httpPath = message.getHeader("CamelHttpPath", String) ?: ""
    def httpQuery = message.getHeader("CamelHttpQuery", String) ?: ""
    def httpMethod = message.getHeader("CamelHttpMethod", String) ?: "GET"

    def response = ""
    def contentType = "text/plain"

    // --- GET-Abfragen ---
    if (httpMethod == "GET") {
        if (httpQuery.contains("1001")) {
            response = '''<BusinessPartner>
    <UUID>123e4567-e89b-12d3-a456-426614174000</UUID>
    <InternalID>1001</InternalID>
    <DeletedIndicator>false</DeletedIndicator>
    <Common>
      <DeletedIndicator>false</DeletedIndicator>
      <Person>
        <Name>
          <GivenName>Max</GivenName>
          <FamilyName>Mustermann</FamilyName>
        </Name>
      </Person>
      <Organisation>
        <Name>
          <FirstLineName>Beispiel GmbH</FirstLineName>
        </Name>
      </Organisation>
    </Common>
    <AddressInformation>
      <UUID>address-001</UUID>
      <Address>
        <PostalAddress>
          <HouseID>42A</HouseID>
        </PostalAddress>
      </Address>
    </AddressInformation>
    <Role>
      <RoleCode>ZCUST</RoleCode>
    </Role>
</BusinessPartner>'''
            contentType = "application/xml"

        } else if (httpQuery.contains("address-001")) {
            response = '''<AddressInformation>
    <UUID>address-001</UUID>
    <Address>
        <PostalAddress>
            <HouseID>42A</HouseID>
        </PostalAddress>
    </Address>
</AddressInformation>'''
            contentType = "application/xml"
        } else {
            response = ""
        }

    // --- POST-Abfragen ---
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
            case "/CreateAddress":
                response = "Address created"
                break
            case "/UpdateAddress":
                response = "Address updated"
                break
            default:
                response = ""
                break
        }

    // --- Unsupported Method ---
    } else {
        response = "Unsupported HTTP method"
    }

    message.setHeader("Content-Type", contentType)
    message.setBody(response)
    return message
}
