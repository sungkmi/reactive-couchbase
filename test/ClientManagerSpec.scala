import com.couchbase.client.java.AsyncBucket
import couchbase.AsyncClient
import org.scalatest.{ BeforeAndAfterAll, TryValues }
import org.scalatestplus.play.{ OneAppPerSuite, PlaySpec }
import play.api.test.FakeApplication

class ClientManagerSpec
    extends PlaySpec
    with OneAppPerSuite
    with TryValues
    with BeforeAndAfterAll {

  implicit override lazy val app: FakeApplication =
    FakeApplication(
      additionalConfiguration = Map("couchbase.password" -> "test123")
    )
  lazy val Clients = new TestClients()

  "ClientManager" must {
    "returns a bucket" in {
      val bucket1 = Clients.getBucket("test")
      bucket1.success.value mustBe a[AsyncBucket]
      Clients.bucketInfo.size === 1

      val bucket2 = Clients.getBucket("test")
      bucket2.success.value mustBe a[AsyncBucket]
      Clients.bucketInfo.size === 1
    }

    "returns a client" in {
      val client1 = Clients.getClient("test")
      client1.success.value mustBe a[AsyncClient]
      Clients.clientInfo.size === 1

      val client2 = Clients.getClient("test")
      client2.success.value mustBe a[AsyncClient]
      Clients.clientInfo.size === 1
    }

  }
  override protected def afterAll(): Unit = {
    Clients.flush("test")
    Clients.shutdown()
    super.afterAll()
  }

}
