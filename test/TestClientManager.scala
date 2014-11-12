import couchbase.{ AsyncClient, BucketInfo, TestHelper, AsyncClientManager }
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.play.PlaySpec

trait TestClientManager extends BeforeAndAfterAll {
  this: PlaySpec =>

  lazy val Buckets = new AsyncClientManager with TestHelper {}

  val testBI = BucketInfo("test", "test123")

  lazy val client = {
    val bucket = Buckets.getBucket(testBI).getOrElse {
      throw new RuntimeException("Failed to get the client")
    }
    new AsyncClient(bucket)
  }

  override protected def afterAll(): Unit = {
    Buckets.flush(testBI)
    Buckets.shutdown()
    super.afterAll()
  }

}
