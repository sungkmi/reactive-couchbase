import com.typesafe.config.ConfigFactory
import couchbase.{ CouchbaseClientManager, BucketInfoManager, BucketInfoManagerImpl }
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.play.PlaySpec
import play.api.Configuration
import play.api.inject._
import play.api.inject.guice.GuiceApplicationBuilder

trait TestApplication extends BeforeAndAfterAll {
  this: PlaySpec =>

  implicit val application = new GuiceApplicationBuilder()
    .configure(new Configuration(ConfigFactory.load("test.conf").resolve()))
    .overrides(bind[BucketInfoManager].to[BucketInfoManagerImpl])
    .overrides(bind[CouchbaseClientManager].to[TestCBClientManagerImpl])
    .build()

  lazy val manager = application.injector.instanceOf[CouchbaseClientManager]

  override protected def afterAll(): Unit = {
    application.stop()
    super.afterAll()
  }
}

