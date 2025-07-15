/* SimpleSuccessMock.groovy
 * Gibt statisch eine Erfolgsmeldung zurück
 * ---------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def response = "<Response>Erfolgreich empfangen</Response>"
    message.setHeader("Content-Type", "application/xml")
    message.setBody(response)
    return message
}
