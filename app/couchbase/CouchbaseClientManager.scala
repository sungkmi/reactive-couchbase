package couchbase

import java.util.concurrent.{ ConcurrentHashMap, CountDownLatch, TimeUnit }
import javax.inject.{ Inject, Singleton }

import com.couchbase.client.java.CouchbaseCluster
import play.api.inject.ApplicationLifecycle
import play.api.{ Logger, Play }

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.{ Failure, Try }

trait CouchbaseClientManager {
  /**
   * Returns the client to access the bucket with the specified name.
   * @param name the name of the bucket
   * @return [[AsyncClient]] if there exists the valid access information for the bucket.
   * @throws NoBucketInformation if there's no access information for the specified name.
   */
  def get(name: String): Try[AsyncClient]

  /**
   * If clean up operations are needed before closing the bucket connections, override this function.
   */
  protected def cleanup(): Unit = {}

  /**
   * Closes the opened connection to the buckets.
   */
  protected def shutdown(): Unit
}

@Singleton
class CouchbaseClientManagerImpl @Inject() (
  lifecycle: ApplicationLifecycle,
  bim: BucketInfoManager)
    extends CouchbaseClientManager {

  lifecycle.addStopHook { () =>
    Future.successful(shutdown())
  }

  protected val servers: java.util.List[String] = {
    val defaultPools: java.util.List[String] = List()
    Play.maybeApplication flatMap { app =>
      app.configuration.getStringList("couchbase.pools")
    } getOrElse {
      Logger.warn("Missing couchbase.pools value: using the default...")
      defaultPools
    }
  }

  protected val cluster = CouchbaseCluster.create(servers)

  protected val clients = new ConcurrentHashMap[String, AsyncClient]()

  def get(name: String): Try[AsyncClient] = Try {
    if (!clients.containsKey(name)) {
      bim.get(name) foreach { bi =>
        val bucket = cluster.openBucket(bi.name, bi.password).async()
        clients.putIfAbsent(name, new AsyncClient(bucket))
        Logger.info(s"Creating a new CouchbaseClient for ${bi.name}...")
      }
    }
    clients.get(name)
  } recoverWith {
    case _: NullPointerException => Failure(new NoBucketInformation(name))
  }

  /**
   * Closes the open buckets and disconnects from the cluster.
   * It blocks the calling thread until the cleanup task is complete.
   */
  protected def shutdown(): Unit = {
    cleanup()

    val latch = new CountDownLatch(clients.size)
    clients.foreach {
      case (n, c) =>
        Logger.info(s"Closing the client for $n bucket...")
        c.bucket.close()
        latch.countDown()
    }
    latch.await()
    Logger.info("Disconnecting from the Couchbase cluster...")

    cluster.disconnect(30L, TimeUnit.SECONDS)
    clients.clear()
  }

}
