import com.couchbase.client.java.document.RawJsonDocument
import couchbase.RawJsonDocumentHelper._
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.play.PlaySpec
import play.api.libs.json.Json

case class LogInfo(providerId: String, providerKey: String)

object LogInfo {
  implicit val format = Json.format[LogInfo]
}

case class User(
  id: String,
  logInfo: LogInfo,
  firstName: Option[String],
  lastName: Option[String],
  avaterUrl: Option[String])

object User {
  implicit val format = Json.format[User]
}

class AsyncClientJSONSpec
    extends PlaySpec
    with ScalaFutures
    with TestApplication {

  val logInfo1 = LogInfo("1000", "facebook")
  val logInfo2 = LogInfo("1000", "google")

  val testUser1 = User(logInfo1.toString, logInfo1, Some("Test1"), Some("User"), None)
  val testUser2 = User(logInfo2.toString, logInfo2, Some("Test2"), Some("User"), None)

  val client = manager.get("test").get

  "AsyncClient" must {
    "create a JSON document" in {
      val content = Json.stringify(Json.toJson(testUser1))
      val doc = RawJsonDocument.create(testUser1.id, content)
      whenReady(client.create(doc)) { doc =>
        doc.toJson.as[User] === testUser1
      }
    }

    "read a JSON document" in {
      whenReady(client.read(RawJsonDocument.create(testUser1.id))) { doc =>
        doc.toJson.as[User] === testUser1
      }
    }

    "update a JSON document" in {
      val updatedUrl = "avatar updated"
      val updatedUser = testUser1.copy(avaterUrl = Some(updatedUrl))
      val updatedDoc = RawJsonDocument.create(updatedUser.id, Json.stringify(Json.toJson(updatedUser)))
      val result = client.update(updatedDoc)
      whenReady(result) { doc =>
        val u: User = doc.toJson.as[User]
        u.avaterUrl.get === updatedUrl
      }
    }

  }
}
