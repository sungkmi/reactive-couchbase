import javax.inject.{ Inject, Singleton }

import com.couchbase.client.java.bucket.AsyncBucketManager
import couchbase.{ BucketInfoManager, CouchbaseClientManagerImpl }
import play.api.inject.ApplicationLifecycle
import rx.lang.scala.JavaConversions.toScalaObservable
import rx.lang.scala.Observable

/**
 * Test helper for AsyncClientManager.
 */
@Singleton
class TestCBClientManagerImpl @Inject() (
  lifeCycle: ApplicationLifecycle,
  bim: BucketInfoManager)
    extends CouchbaseClientManagerImpl(lifeCycle, bim) {

  /**
   * If clean up operations are needed before closing the bucket connections, override this function.
   */
  override protected def cleanup(): Unit = {
    import scala.collection.JavaConversions._
    clients.values().foreach { c =>
      val observable: Observable[AsyncBucketManager] = c.bucket.bucketManager()
      observable.flatMap(_.flush()).toBlocking.first
    }
    super.cleanup()
  }
}
