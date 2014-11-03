import com.couchbase.client.java.document.JsonStringDocument
import couchbase.JsonStringDocumentHelper._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.play.{ OneAppPerSuite, PlaySpec }
import play.api.libs.json.Json
import play.api.test.FakeApplication

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
    with OneAppPerSuite
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit override lazy val app: FakeApplication = FakeApplication(
    additionalConfiguration = Map("couchbase.password" -> "test123")
  )

  lazy val Clients = new TestClients()
  lazy val client = Clients.getClient("test").getOrElse(throw new RuntimeException("Failed to get the client"))

  override protected def afterAll(): Unit = {
    Clients.flush("test")
    Clients.shutdown()
    super.afterAll()
  }

  val logInfo1 = LogInfo("1000", "facebook")
  val logInfo2 = LogInfo("1000", "google")

  val testUser1 = User(logInfo1.toString, logInfo1, Some("Test1"), Some("User"), None)
  val testUser2 = User(logInfo2.toString, logInfo2, Some("Test2"), Some("User"), None)

  "AsyncClient" must {
    "create a JSON document" in {
      val content = Json.stringify(Json.toJson(testUser1))
      val doc = JsonStringDocument.create(testUser1.id, content)
      whenReady(client.create(doc)) { doc =>
        doc.toJson.as[User] === testUser1
      }
    }

    "read a JSON document" in {
      whenReady(client.read(JsonStringDocument.create(testUser1.id))) { doc =>
        doc.toJson.as[User] === testUser1
      }
    }

    "update a JSON document" in {
      val updatedUrl = "avatar updated"
      val updatedUser = testUser1.copy(avaterUrl = Some(updatedUrl))
      val updatedDoc = JsonStringDocument.create(updatedUser.id, Json.stringify(Json.toJson(updatedUser)))
      val result = client.update(updatedDoc)
      whenReady(result) { doc =>
        val u: User = doc.toJson.as[User]
        u.avaterUrl.get === updatedUrl
      }
    }

  }
}
