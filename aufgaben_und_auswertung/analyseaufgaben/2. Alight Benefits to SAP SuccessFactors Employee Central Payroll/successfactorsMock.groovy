/* SimplePassthroughMock.groovy
 * Gibt den empfangenen Payload 1:1 zurück
 * -------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def body = message.getBody(String)
    message.setBody(body)
    return message
}
