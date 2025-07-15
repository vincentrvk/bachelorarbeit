/* BatchGetGeocodeMock.groovy
 * Gibt eine feste BatchGetGeocodeResponse im XML-Format zurück
 * ------------------------------------------------------------ */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def response = '''<ns0:BatchGetGeocodeResponse xmlns:ns0="http://soa.noventic.com/GeocodeService/GeocodeService-V1">
  <ns0:BatchGetGeocodeResult>
    <ns0:GeocodeResponseList>
      <ns0:GeocodeResponse>
        <ns0:Geocode>123456789</ns0:Geocode>
        <ns0:SequenceNum>1</ns0:SequenceNum>
        <ns0:ResponseCode>0</ns0:ResponseCode>
        <ns0:ErrorMessage></ns0:ErrorMessage>
      </ns0:GeocodeResponse>
      <ns0:GeocodeResponse>
        <ns0:Geocode>987654321</ns0:Geocode>
        <ns0:SequenceNum>2</ns0:SequenceNum>
        <ns0:ResponseCode>100</ns0:ResponseCode>
        <ns0:ErrorMessage>Invalid ZIP code</ns0:ErrorMessage>
      </ns0:GeocodeResponse>
    </ns0:GeocodeResponseList>
  </ns0:BatchGetGeocodeResult>
</ns0:BatchGetGeocodeResponse>'''

    message.setHeader("Content-Type", "application/xml")
    message.setBody(response)
    return message
}
