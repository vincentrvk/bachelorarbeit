/* CsvResponseMock.groovy
 * Gibt einen festen CSV-Payload zurück
 * ----------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def csv = """\
PR_Creation_List,PR_PO_Status,Created_By,Request_ID,PR_Number,Unique_Key,Request_Name,Contract_Reference,Anaplan_Line_Number,PR_Line_Number,Quantity,Price,Supplier_Code,SKU,Deliver_To_Code,Need_By_Date
ListA,Open,j.smith,REQ-001,PR-1001,UK-001,Office Supplies,CR-001,ALN-01,1,10,15.99,SUP-001,SKU-001,LOC-001,2025-06-01
ListB,Approved,a.jones,REQ-002,PR-1002,UK-002,IT Equipment,CR-002,ALN-02,2,5,299.50,SUP-002,SKU-002,LOC-002,2025-07-15
ListC,Change,m.taylor,REQ-003,PR-1001,UK-003,Marketing Materials,CR-003,ALN-03,3,20,8.75,SUP-003,SKU-003,LOC-003,2025-08-10
""".stripIndent()

    message.setHeader("Content-Type", "text/csv")
    message.setBody(csv)

    return message
}
