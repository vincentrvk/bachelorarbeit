import com.sap.gateway.ip.core.customdev.util.Message

def Message processData(Message message) {
    def groovyVersion = GroovySystem.version
    def javaVersion = System.getProperty("java.version")
    def javaVendor  = System.getProperty("java.vendor")

    def body = "Groovy-Version: ${groovyVersion}\nJava-Version: ${javaVersion}\nJava-Vendor: ${javaVendor}"
    message.setBody(body)
    return message
}
