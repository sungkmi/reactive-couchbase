import com.couchbase.client.java.document.JsonStringDocument
import com.couchbase.client.java.error.{ DocumentAlreadyExistsException, DocumentDoesNotExistException }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.play.{ OneAppPerSuite, PlaySpec }
import play.api.test.FakeApplication

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class AsyncClientBasicSpec
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

  val testId1 = "test::0001"
  val testContent1 = "Test document 0001"
  val testId2 = "test::0002"
  val testContent2 = "Test document 0002"
  val testId3 = "test::0003"
  val testContent3 = "Test document 0003"

  "AsyncClient" must {
    "create a new document and return the created document when everything goes well." in {
      whenReady(client.create(testId1, testContent1)) { doc =>
        doc.id() === testId1
        doc.content() === testContent1
      }
    }

    "not allow the creation of a document when a document already exists with the same id." in {
      val result = client.create(testId1, testContent1)
      whenReady(result.failed) { e =>
        e mustBe a[DocumentAlreadyExistsException]
      }
    }

    "read a document and return the read document." in {
      whenReady(client.read(testId1)) { doc =>
        doc.id() === testId1
        doc.content() === testContent1
      }
    }

    "return a failed future when the document does not exist for the given id." in {
      val result = client.read("invalid")
      whenReady(result.failed) { e =>
        e mustBe a[DocumentDoesNotExistException]
      }
    }

    "read multiple documents" in {
      val result: Future[Map[String, String]] = for {
        doc2 <- client.create(testId2, testContent2)
        doc3 <- client.create(testId3, testContent3)
        list <- client.read(Seq(testId1, testId2, testId3))
      } yield {
        list.map(d => d.id() -> d.content()).toMap
      }

      whenReady(result) { m =>
        m must contain allOf (testId1 -> testContent1, testId2 -> testContent2, testId3 -> testContent3)
      }
    }

    "return an empty list when the documents with the specified keys don't exist." in {
      val result = client.read(Seq("invalid1", "invalid2", "invalid3"))
      whenReady(result) { l =>
        l mustBe empty
      }
    }

    "filter the duplicated keys when reading multiple documents." in {
      val result = client.read(Seq(testId1, testId1, testId2, testId2))
      whenReady(result) { docs =>
        docs.size === 2
      }
    }

    "update the document" in {
      val result = client.update(testId2)(_ => "updated")
      whenReady(result) { doc =>
        doc.id() === testId2
        doc.content() === "updated"
      }
    }

    "return a failed future when the id of the board to be updated is invalid." in {
      val result = client.update("invalid")(_ => "invalid")
      whenReady(result.failed) { e =>
        e mustBe a[DocumentDoesNotExistException]
      }
    }

    "replace the document." in {
      val result = client.replace(testId3, "replaced")
      whenReady(result) { doc =>
        doc.content() === "replaced"
      }
    }

    "return a failed future when the id of the document to be replaced is invalid." in {
      val result = client.replace("invalid", "invalid")
      whenReady(result.failed) { e =>
        e mustBe a[DocumentDoesNotExistException]
      }
    }

    "delete the specified file." in {
      val result = client.delete(testId1, testId2, testId3)
      whenReady(result) { docs =>
        docs.size === 3
      }
    }

    "ignore the wrong id when deleting." in {
      val result = client.delete("invalid")
      whenReady(result) { docs =>
        docs.size === 0
        docs.isEmpty === true
      }
    }
  }

}
