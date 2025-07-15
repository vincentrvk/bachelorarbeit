/* SoapResponseMock.groovy
 * Gibt einen festen SOAP-ähnlichen XML-Payload zurück
 * -------------------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def responseXml = '''<SoapRequestResponse xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <SoapRequestResult>
    <GroupList>
      <Group>
        <LineNumber>001</LineNumber>
        <TaxList>
          <Tax>
            <TaxTypeCode>VAT</TaxTypeCode>
            <ExemptCode>EX01</ExemptCode>
            <TaxAmount>15.00</TaxAmount>
            <Revenue>100.00</Revenue>
            <PercentTaxable>0.15</PercentTaxable>
            <NumberOfTaxes>2</NumberOfTaxes>
            <NumberOfGroups>1</NumberOfGroups>
          </Tax>
          <Tax>
            <TaxTypeCode>LOCAL</TaxTypeCode>
            <ExemptCode>EX02</ExemptCode>
            <TaxAmount>5.00</TaxAmount>
            <Revenue>50.00</Revenue>
            <PercentTaxable>0.10</PercentTaxable>
            <NumberOfTaxes>2</NumberOfTaxes>
            <NumberOfGroups>1</NumberOfGroups>
          </Tax>
        </TaxList>
      </Group>
    </GroupList>
  </SoapRequestResult>
</SoapRequestResponse>'''

    message.setHeader("Content-Type", "application/xml")
    message.setBody(responseXml)

    return message
}
