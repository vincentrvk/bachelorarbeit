/* GetAllGeocodesMock.groovy
 * Gibt statisch definierte Geocodes im XML-Format zurück
 * ------------------------------------------------------ */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def response = '''<ns0:GetAllGeocodesResponse xmlns:ns0="http://tempuri.org/">
  <ns0:GetAllGeocodesResult>
    <ns0:string>US1234512331</ns0:string>
    <ns0:string>ZZ0333367890</ns0:string>
    <ns0:string>ZZ9999999999</ns0:string>
  </ns0:GetAllGeocodesResult>
</ns0:GetAllGeocodesResponse>'''

    message.setHeader("Content-Type", "application/xml")
    message.setBody(response)
    return message
}
