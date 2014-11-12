package couchbase

import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch, TimeUnit }

import com.couchbase.client.java.{ AsyncBucket, CouchbaseCluster }
import play.api.{ Logger, Play }

import scala.collection.JavaConversions._
import scala.util.Try

trait ClientManager {
  def getBucket(bi: BucketInfo): Try[AsyncBucket]

  def shutdown(): Unit
}

class AsyncClientManager extends ClientManager {

  val servers: java.util.List[String] = {
    val defaultPools: java.util.List[String] = List()
    Play.maybeApplication flatMap { app =>
      app.configuration.getStringList("couchbase.pools")
    } getOrElse {
      Logger.warn("Missing couchbase.pools value: using the default...")
      defaultPools
    }
  }

  protected val cluster = CouchbaseCluster.create(servers)

  protected val buckets = new ConcurrentHashMap[String, AsyncBucket]()

  /**
   * Returns the AsyncBucket of the given name.
   * @param bi the bucket name and the password to access the bucket.
   * @return AsyncBucket
   */
  def getBucket(bi: BucketInfo): Try[AsyncBucket] = Try {
    if (!buckets.containsKey(bi.name)) {
      val bucket = cluster.openBucket(bi.name, bi.password).async()
      buckets.putIfAbsent(bi.name, bucket)
      Logger.info(s"Creating a new CouchbaseClient for ${bi.name}...")
    }
    buckets.get(bi.name)
  }

  /**
   * Closes the open buckets and disconnects from the cluster.
   * It blocks the calling thread until the cleanup task is complete.
   */
  def shutdown(): Unit = {
    val latch = new CountDownLatch(buckets.size())

    buckets.foreach {
      case (name, bucket) =>
        Logger.info(s"Closing the $name bucket...")
        bucket.close()
        latch.countDown()
    }
    latch.await()
    Logger.info("Disconnecting from the Couchbase cluster...")
    cluster.disconnect(30L, TimeUnit.SECONDS)
    buckets.clear()
  }

}
