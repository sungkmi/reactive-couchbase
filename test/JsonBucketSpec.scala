import couchbase.JsonBucketManager
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{JsValue, Json}
import play.api.test.FakeApplication

import scala.language.implicitConversions

class JsonBucketSpec
  extends PlaySpec
  with OneAppPerSuite
  with ScalaFutures
  with BeforeAndAfterAll {

  implicit override lazy val app: FakeApplication =
    FakeApplication(
      additionalConfiguration = Map("couchbase.password" -> "test123")
    )

  lazy val Buckets = new JsonBucketManager {}

  def Bucket = Buckets.get("test").get

  case class Document(title: String,
                      contents: String,
                      created: DateTime,
                      modified: DateTime)

  implicit val format = Json.format[Document]

  implicit def doc2Json(doc: Document): JsValue = format.writes(doc)

  val testKey = "test::0001"
  val testDoc = Document("Test Title", "Test Contents", DateTime.now(), DateTime.now())

  "JsonClient" must {
    "create a JSON document" in {
      whenReady(Bucket.createJson(testKey, testDoc)) {
        result =>
          result must not be(None)
      }
    }

    "update a JSON document" in {
      val updated = testDoc.copy(contents = "Test Contents Updated")
      whenReady(Bucket.updateJson(testKey, updated)) {
        result =>
          result.asOpt[Document] must not be(None)
          val doc = result.as[Document]
          doc.created !== doc.modified
          doc.contents === "Test Contents Updated"
      }
    }

    "read a JSON document" in {
      whenReady(Bucket.readJson(testKey)) {
        result =>
          result.asOpt[Document] must not be(None)
          val doc = result.as[Document]
          doc.title === testDoc.title
          doc.created === testDoc.created
      }
    }

    "delete a JSON document" in {
      whenReady(Bucket.delete(testKey)) {
        r =>
          r === true
      }
    }
  }

  override protected def afterAll(): Unit = {
    Bucket.client.flush().get()
    Buckets.shutdown()
    super.afterAll()
  }
}
