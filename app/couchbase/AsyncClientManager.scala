package couchbase

import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch, TimeUnit }
import javax.inject.Singleton

import com.couchbase.client.java.{ AsyncBucket, CouchbaseCluster }
import play.api.{ Logger, Play }

import scala.collection.JavaConversions._
import scala.util.Try

trait ClientManager {
  def getBucket(name: String): Try[AsyncBucket]

  def getClient(name: String): Try[AsyncClient]

  def shutdown(): Unit
}

@Singleton
class AsyncClientManager extends ClientManager {
  import play.api.Play.current

  val servers: java.util.List[String] = {
    val defaultPools: java.util.List[String] = List()
    Play.configuration.getStringList("couchbase.pools").getOrElse {
      Logger.warn("Missing couchbase.pools value: using the default...")
      defaultPools
    }
  }

  val password = Play.configuration.getString("couchbase.password").getOrElse {
    Logger.warn("Missing couchbase.password value: using the default...")
    ""
  }

  protected val cluster = CouchbaseCluster.create(servers)

  protected val buckets = new ConcurrentHashMap[String, AsyncBucket]()

  protected val clients = new ConcurrentHashMap[String, AsyncClient]()

  /**
   * Returns the AsyncBucket of the given name.
   * @param name the bucket name
   * @return AsyncBucket
   */
  def getBucket(name: String): Try[AsyncBucket] = Try {
    if (!buckets.containsKey(name)) {
      val bucket = cluster.openBucket(name, password).async()
      buckets.putIfAbsent(name, bucket)
      Logger.info(s"Creating a new CouchbaseClient for $name...")
    }
    buckets.get(name)
  }

  /**
   * Returns the AsyncClient to connect the bucket of the given name.
   * @param name the bucket name
   * @return AsyncClient
   */
  def getClient(name: String): Try[AsyncClient] = {
    getBucket(name) map { bucket =>
      if (!clients.containsKey(name)) {
        clients.putIfAbsent(name, new AsyncClient(bucket))
        Logger.info(s"Creating a new AsyncClient for $name...")
      }
      clients.get(name)
    }
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
    clients.clear()
    buckets.clear()
  }

}
