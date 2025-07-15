/* KeywordQueryAssetMock.groovy
 * Gibt JSON mit Asset-Daten zurück, wenn Header 'keyword_query' vorhanden ist
 * --------------------------------------------------------------------------- */
import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    def keywordQuery = message.getHeader("keyword_query", String)

    def response

    if (keywordQuery) {
        response = '''{
  "assetsList": {
    "assets": [
      {
        "asset": {
          "assetId": "asset-001",
          "description": "Produktbild Sommerkollektion 2025",
          "variants": [
            {
              "fileId": "file-1001",
              "url": "https://cdn.example.com/assets/asset-001/preview.jpg"
            },
            {
              "fileId": "file-1002",
              "url": "https://cdn.example.com/assets/asset-001/thumbnail.jpg"
            }
          ],
          "keywords": "sommer,mode,damen"
        }
      },
      {
        "asset": {
          "assetId": "asset-002",
          "description": "Produktbild Winterkollektion 2025",
          "variants": [
            {
              "fileId": "file-2001",
              "url": "https://cdn.example.com/assets/asset-002/preview.jpg"
            },
            {
              "fileId": "file-2002",
              "url": "https://cdn.example.com/assets/asset-002/thumbnail.jpg"
            }
          ],
          "keywords": "winter,mode,damen"
        }
      }
    ]
  }
}'''
        message.setHeader("Content-Type", "application/json")
    } else {
        response = "connected"
        message.setHeader("Content-Type", "text/plain")
    }

    message.setBody(response)
    return message
}
