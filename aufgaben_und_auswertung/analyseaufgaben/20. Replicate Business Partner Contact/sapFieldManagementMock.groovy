/* SimplePassthroughMock.groovy
 * Nimmt den Payload entgegen und tut sonst nichts
 * ----------------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    // Hole den eingehenden Body (egal ob XML, JSON, Text …)
    def body = message.getBody(String)
    
    // Gib ihn unverändert zurück
    message.setBody(body)
    return message
}
