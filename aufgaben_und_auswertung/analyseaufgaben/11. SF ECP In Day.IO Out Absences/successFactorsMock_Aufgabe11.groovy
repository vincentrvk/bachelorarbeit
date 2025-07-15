/* ZHR_FGRP_0001ResponseMock.groovy
 * Gibt eine feste SAP RFC XML-Antwort zurück
 * ------------------------------------------ */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def response = '''<?xml version="1.0" encoding="UTF-8"?>
<ns1:ZHR_FGRP_0001Response xmlns:ns1="urn:sap-com:document:sap:rfc:functions">
  <ns1:RETURN>
    <ns1:item>
      <ns1:STATUS>S</ns1:STATUS>
      <ns1:MENSAGEM>Vorgang erfolgreich verarbeitet.</ns1:MENSAGEM>
    </ns1:item>
    <ns1:item>
      <ns1:STATUS>E</ns1:STATUS>
      <ns1:MENSAGEM>Fehlerhafte Personalnummer.</ns1:MENSAGEM>
    </ns1:item>
  </ns1:RETURN>
</ns1:ZHR_FGRP_0001Response>'''

    message.setHeader("Content-Type", "application/xml")
    message.setBody(response)
    return message
}
