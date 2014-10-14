import couchbase.{JsonBucket, JsonBucketManager}
import org.scalatest.{TryValues, BeforeAndAfterAll}
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}
import play.api.test.FakeApplication

import scala.util.Try

class BucketManagerSpec
  extends PlaySpec
  with OneAppPerSuite
  with TryValues
  with BeforeAndAfterAll {

  implicit override lazy val app: FakeApplication =
    FakeApplication(
      additionalConfiguration = Map("couchbase.password" -> "test123")
    )

  lazy val Buckets = new JsonBucketManager {}

  "BucketManager" must {
    "returns the Bucket." in {
      Buckets.buckets.size() === 0
      val bucket: Try[JsonBucket] = Buckets.get("test")
      bucket.success.value mustBe a [JsonBucket]
      Buckets.buckets.size() === 1
    }

    "throws an ConfigurationException when invalid bucket name is provided." in {
      val bucket = Buckets.get("invalid")
      bucket.failure.exception mustBe a [com.couchbase.client.vbucket.ConfigurationException]
      Buckets.buckets.size() === 1
    }
  }

  override protected def afterAll(): Unit = {
    Buckets.shutdown()
    super.afterAll()
  }

}
