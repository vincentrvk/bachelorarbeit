/* ConditionalSoapMock.groovy
 * Gibt je nach Vorhandensein von //ns4:PrimaryAddressLine unterschiedliche XML-Payloads zurück
 * ------------------------------------------------------------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def body = message.getBody(String)

    // Primitive Prüfung auf Vorhandensein von ns4:PrimaryAddressLine per String-Suche
    def hasPrimaryAddressLine = body.contains("<ns4:PrimaryAddressLine>")

    def response
    if (hasPrimaryAddressLine) {
        response = '''<ns0:SoapRequestResponse xmlns:ns0="http://sureaddress.net/">
  <ns0:SoapRequestResult>
    <ns0:State>CA</ns0:State>
    <ns0:GeoCode>123456</ns0:GeoCode>
    <ns0:PrimaryAddressLine>1600 Amphitheatre Parkway</ns0:PrimaryAddressLine>
  </ns0:SoapRequestResult>
</ns0:SoapRequestResponse>'''
    } else {
        response = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:ns1="http://service.example.com/xsd">
  <soapenv:Body>
    <ns1:GetGeocodeByZipcodeOrAddressResponse>
      <ns1:Geocode>52.5200,13.4050</ns1:Geocode>
      <ns1:StateCode>BE</ns1:StateCode>
    </ns1:GetGeocodeByZipcodeOrAddressResponse>
  </soapenv:Body>
</soapenv:Envelope>'''
    }

    message.setHeader("Content-Type", "application/xml")
    message.setBody(response)

    return message
}
