import couchbase.AsyncClient
import org.scalatest.TryValues
import org.scalatestplus.play.PlaySpec

class ClientManagerSpec
    extends PlaySpec
    with TryValues
    with TestApplication {

  "ClientManager" must {
    "returns the clients." in {
      manager.get("test").success.value mustBe a[AsyncClient]
      manager.get("cache").success.value mustBe a[AsyncClient]
    }
  }

}
