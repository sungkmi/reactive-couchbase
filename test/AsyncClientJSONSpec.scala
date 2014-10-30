import couchbase.AsyncClient._
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
      whenReady(client.create(testUser1.id, content)) { doc =>
        doc.toJson.as[User] === testUser1
      }
    }

    "read a JSON document" in {
      whenReady(client.read(testUser1.id)) { doc =>
        doc.toJson.as[User] === testUser1
      }
    }

    "update a JSON document" in {
      val updatedUrl = "avatar updated"
      val result = client.update(testUser1.id) { str =>
        val u = Json.parse(str).as[User](User.format)
        val updated = u.copy(avaterUrl = Some(updatedUrl))
        Json.stringify(Json.toJson(updated))
      }
      whenReady(result) { doc =>
        val u = doc.toJson.as[User](User.format)
        u.avaterUrl === updatedUrl
      }
    }

    "throw a failed future if updating the JSON content fails." in {
      val result = client.update(testUser1.id) { str =>
        throw new RuntimeException("failed to transform to JSON")
        "invalid"
      }
      whenReady(result.failed) { e =>
        e mustBe a[RuntimeException]
      }
    }

  }
}
