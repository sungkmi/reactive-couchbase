package couchbase

import com.couchbase.client.java.bucket.AsyncBucketManager
import play.api.Logger
import rx.lang.scala.JavaConversions.toScalaObservable
import rx.lang.scala.Observable

import scala.util.Try

/**
 * Test helper for AsyncClientManager.
 * Don't use in the production environment.
 */
trait TestHelper {
  this: AsyncClientManager =>

  import scala.collection.JavaConversions._

  def bucketInfo = {
    buckets.keys().toIterable
  }

  /**
   * Flushes the bucket, deletes everything in the specified bucket.
   * @param bi the bucket name and the password to access the bucket
   * @return The result of the flushing.
   */
  def flush(bi: BucketInfo): Boolean = {
    Logger.info(s"Flushing the ${bi.name} bucket...")
    val result: Try[Boolean] = getBucket(bi) map {
      bucket =>
        val observable: Observable[AsyncBucketManager] = bucket.bucketManager()
        observable
          .flatMap(_.flush())
          .toBlocking
          .first
    }
    result.getOrElse(false)
  }

}
