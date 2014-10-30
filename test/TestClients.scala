import java.lang

import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.bucket.AsyncBucketManager
import couchbase.AsyncClientManager
import play.api.Logger
import rx.functions.Func1
import rx.{ Observable, Subscriber }

import scala.concurrent.{ Future, Promise }
import scala.util.Try

class TestClients extends AsyncClientManager {

  import scala.collection.JavaConversions._

  def bucketInfo = {
    buckets.keys().toIterable
  }

  def clientInfo = {
    clients.keys().toIterable
  }

  /**
   * Flushes the bucket, deletes everything in the specified bucket.
   * @param name the bucket name
   * @return The result of the flushing.
   */
  def flush(name: String): Boolean = {
    Logger.info(s"Flushing the $name bucket...")
    val result: Try[Boolean] = getBucket(name) map {
      bucket =>
        bucket
          .bucketManager()
          .flatMap(
            new Func1[AsyncBucketManager, Observable[java.lang.Boolean]] {
              override def call(bucketManager: AsyncBucketManager): Observable[lang.Boolean] = {
                bucketManager.flush()
              }
            })
          .toBlocking
          .first()
    }
    result.getOrElse(false)
  }

}
