import com.couchbase.client.java.document.StringDocument
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.play.PlaySpec

class MemcacheSpec
    extends PlaySpec
    with BeforeAndAfterAll
    with ScalaFutures
    with TestClientManager {

  "Memcache Client" must {
    "create a new document and return the created document." in {
      whenReady(cache.create(StringDocument.create("test", "cached"))) { doc =>
        doc.id() === "test"
        doc.content() === "cached"
      }
    }

    "read a document." in {
      whenReady(cache.read(StringDocument.create("test"))) { doc =>
        doc.id() === "test"
        doc.content() === "cached"
      }
    }

    "update a document." in {
      whenReady(cache.update(StringDocument.create("test", "updated"))) { doc =>
        doc.id() === "test"
        doc.content() === "updated"
      }
    }

    "delete a document." in {
      whenReady(cache.delete("test")) { id =>
        id === "test"
      }
    }
  }
}
