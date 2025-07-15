/* GoldenTaxMock.groovy
 * Gibt GoldenTax XML nur bei POST mit Pfad-ID 123456789012345 zurück
 * ------------------------------------------------------------------ */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def httpMethod = message.getHeader("CamelHttpMethod", String) ?: ""
    def httpPath = message.getHeader("CamelHttpPath", String) ?: ""

    def response = ""
    def contentType = "application/xml"
    def statusCode = 200

    if (httpMethod == "POST" && httpPath.contains("123456789012345")) {
        response = '''<?xml version="1.0" encoding="UTF-8"?>
<GoldenTax xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <Control>
    <TaxNumber>123456789012345</TaxNumber>
    <TerminalID>TERM001</TerminalID>
    <SourceSystem>SAP</SourceSystem>
    <GTDocument>
      <GTHeader>
        <GoldenTaxNumber>GTX0001</GoldenTaxNumber>
        <InvoiceType>N</InvoiceType>
        <InvoiceMedium>E</InvoiceMedium>
        <GoodsList>1</GoodsList>
        <NetAmount>1500.00</NetAmount>
        <TaxAmount>225.00</TaxAmount>
        <GTItem>
          <GoldenTaxItemNumber>001</GoldenTaxItemNumber>
          <GoodsName>Notebook</GoodsName>
          <UoM>PCS</UoM>
          <NetAmount>1000.00</NetAmount>
          <TaxRate>15.00</TaxRate>
          <TaxClassificationNumber>1010101</TaxClassificationNumber>
        </GTItem>
        <GTItem>
          <GoldenTaxItemNumber>002</GoldenTaxItemNumber>
          <GoodsName>Mouse</GoodsName>
          <UoM>PCS</UoM>
          <NetAmount>500.00</NetAmount>
          <TaxRate>15.00</TaxRate>
          <TaxClassificationNumber>1010102</TaxClassificationNumber>
        </GTItem>
      </GTHeader>
      <GTHeader>
        <GoldenTaxNumber>GTX0002</GoldenTaxNumber>
        <InvoiceType>S</InvoiceType>
        <InvoiceMedium>P</InvoiceMedium>
        <GoodsList>1</GoodsList>
        <NetAmount>3000.00</NetAmount>
        <TaxAmount>450.00</TaxAmount>
        <GTItem>
          <GoldenTaxItemNumber>003</GoldenTaxItemNumber>
          <GoodsName>Monitor</GoodsName>
          <UoM>PCS</UoM>
          <NetAmount>2000.00</NetAmount>
          <TaxRate>15.00</TaxRate>
          <TaxClassificationNumber>1010201</TaxClassificationNumber>
        </GTItem>
        <GTItem>
          <GoldenTaxItemNumber>004</GoldenTaxItemNumber>
          <GoodsName>Keyboard</GoodsName>
          <UoM>PCS</UoM>
          <NetAmount>1000.00</NetAmount>
          <TaxRate>15.00</TaxRate>
          <TaxClassificationNumber>1010202</TaxClassificationNumber>
        </GTItem>
      </GTHeader>
    </GTDocument>
  </Control>
</GoldenTax>'''
    } else {
        statusCode = 404
        response = ""
    }

    message.setHeader("Content-Type", contentType)
    message.setHeader("CamelHttpResponseCode", statusCode)
    message.setBody(response)
    return message
}
