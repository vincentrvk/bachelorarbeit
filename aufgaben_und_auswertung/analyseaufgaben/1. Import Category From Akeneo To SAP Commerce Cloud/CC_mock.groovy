/* CC_Mock.groovy
 * Bestätigt jede eingehende XML‑Payload mit HTTP 201
 * ------------------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {

    // Eingehendes XML für spätere Kontrolle anhängen (optional)
    def msgLog = messageLogFactory.getMessageLog(message)
    msgLog?.addAttachmentAsString(
        'ReceivedFromAkeneoScript',
        message.getBody(String) ?: '',
        'application/xml'
    )

    /* -------- Response vorbereiten -------------------------------------- */
    message.setHeader('Content-Type', 'text/plain')
    message.setHeader('SAP_HTTP_ResponseCode', 201 as String)  // CPI‑eigene Header‑Variante
    message.setBody('')                                        // leerer Body

    return message
}
