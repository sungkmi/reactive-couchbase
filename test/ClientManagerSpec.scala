import com.couchbase.client.java.AsyncBucket
import couchbase.{ BucketInfo, TestHelper, AsyncClientManager, AsyncClient }
import org.scalatest.{ BeforeAndAfterAll, TryValues }
import org.scalatestplus.play.{ OneAppPerSuite, PlaySpec }
import play.api.test.FakeApplication

class ClientManagerSpec
    extends PlaySpec
    with TryValues
    with BeforeAndAfterAll {

  lazy val Clients = new AsyncClientManager with TestHelper {}
  val testBI = BucketInfo("test", "test123")

  "BucketManager" must {
    "returns a bucket" in {
      val bucket1 = Clients.getBucket(testBI)
      bucket1.success.value mustBe a[AsyncBucket]
      Clients.bucketInfo.size === 1

      val bucket2 = Clients.getBucket(testBI)
      bucket2.success.value mustBe a[AsyncBucket]
      Clients.bucketInfo.size === 1
    }
  }

  override protected def afterAll(): Unit = {
    Clients.flush(testBI)
    Clients.shutdown()
    super.afterAll()
  }

}
