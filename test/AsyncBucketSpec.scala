import couchbase.CouchbaseExceptions.{DeleteException, ReadException}
import couchbase.JsonBucketManager
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}
import play.api.Logger
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.test.FakeApplication

class AsyncBucketSpec
  extends PlaySpec
  with OneAppPerSuite
  with ScalaFutures
  with BeforeAndAfterAll {

  implicit override lazy val app: FakeApplication =
    FakeApplication(
      additionalConfiguration = Map("couchbase.password" -> "test123")
    )

  lazy val Buckets = new JsonBucketManager()

  def Bucket = Buckets.get("test")

  val testKey = "u::0001"
  val testDoc = "User Information 1"

  "AsyncClient" must {
    "create a document" in {
      whenReady(Bucket.create(testKey, testDoc)) {
        r =>
          r mustBe a[String]
          r === testDoc
      }
    }

    "read a document." in {
      whenReady(Bucket.read(testKey)) {
        r =>
          r mustBe a[String]
          r === "User Information 1 is updated"
      }
    }

  }

  "throw a ReadException when non-existent doc is being read." in {
    val key = "invalid"
    whenReady(Bucket.read(key).failed) {
      t =>
        t mustBe a[ReadException]
    }
  }

  "delete a document." in {
    whenReady(Bucket.delete(testKey)) {
      r =>
        r === true
    }
  }

  "throw a DeleteException when non-existent doc is being deleted." in {
    val key = "invalid"
    whenReady(Bucket.delete(key).failed) {
      t =>
        t mustBe a[DeleteException]
    }
  }

  "read multiple documents" in {
    val keys = (1 to 100).map(i => f"doc::$i%04d")
    val docs = (1 to 100).map(i => f"doc contents $i%04d")
    for {
      (k, v) <- keys zip docs
    } yield {
      Bucket.create(k, v)
    }

    whenReady(Bucket.readBulk(keys)) {
      m =>
        Logger.debug("Printing the read documents...")
        m foreach { case (k, v) => Logger.debug(f"$k%15s : $v%s")}
        m must have size (keys.length)
    }
  }

  "ignore missing documents when reading multiple documents" in {
    val keys = (1 to 100).map(i => f"missing::$i%04d")
    val docs = (1 to 50).map(i => f"doc contents $i%04d")
    for {
      (k, v) <- keys zip docs
    } yield {
      Bucket.create(k, v)
    }

    whenReady(Bucket.readBulk(keys)) {
      m =>
        Logger.debug("Printing the read documents...")
        m foreach { case (k, v) => Logger.debug(f"$k%15s : $v%s")}
        m must have size (docs.length)
    }
  }
  "increase a counter" in {
    val counterKey = "u::counter1"
    whenReady(Bucket.incr(counterKey, 1, 1000)) {
      c =>
        c === 1000L
        whenReady(Bucket.incr(counterKey, 1)) {
          c =>
            c === 1001L
        }
    }
  }

  "decrease a counter" in {
    val counterKey = "u::counter2"
    whenReady(Bucket.decr(counterKey, 1, 0)) {
      c =>
        c === 0L
        whenReady(Bucket.decr(counterKey, 1)) {
          c =>
            c === -1L
        }
    }
  }


  override protected def afterAll(): Unit = {
    Bucket.client.flush().get()
    Buckets.shutdown()
    super.afterAll()

  }
}