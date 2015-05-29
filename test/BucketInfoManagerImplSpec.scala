import com.typesafe.config.ConfigFactory
import couchbase.{ BucketInfo, BucketInfoManagerImpl, BucketInfoManager }
import org.scalatestplus.play.PlaySpec
import play.api.Configuration
import play.api.inject.bind
import play.api.inject.guice.GuiceApplicationBuilder

class BucketInfoManagerImplSpec extends PlaySpec with TestApplication {
  "BucketInfoManager" must {
    "read the configuration." in {
      application.configuration.getConfigList("couchbase.buckets").size === 2
    }

    "be instantiated." in {
      val bim = application.injector.instanceOf[BucketInfoManager]
      bim mustBe a[BucketInfoManagerImpl]
    }

    "return the access information for valid keys." in {
      val bim = application.injector.instanceOf[BucketInfoManager]
      bim.gets().size === 2
      bim.get("test") === Some(BucketInfo("test", "test123"))
      bim.get("cache") === Some(BucketInfo("cache", ""))
    }

    "return None for invalid keys." in {
      val bim = application.injector.instanceOf[BucketInfoManager]
      bim.get("invalid") === None
    }
  }
}
