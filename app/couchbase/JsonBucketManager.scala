package couchbase

import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.couchbase.client.CouchbaseClient
import play.api.Play.current
import play.api.{Logger, Play}

import scala.collection.JavaConversions._
import scala.util.Try

class JsonBucketManager {
  /**
   * Holds the Couchbase client.
   */
  val buckets = new ConcurrentHashMap[String, JsonBucket]()

  val servers: java.util.List[URI] = {
    val defaultPools: java.util.List[String] = List("http://localhost:8091/pools")
    val pools: java.util.List[String] = Play.configuration.getStringList("couchbase.pools").getOrElse {
      Logger.warn("Missing couchbase.pools value: using the default...")
      defaultPools
    }
    pools.map(URI.create(_))
  }

  val password = Play.configuration.getString("couchbase.password").getOrElse {
    Logger.warn("Missing couchbase.password value: using the default...")
    ""
  }


  /**
   * Returns a JsonBucket for the given name.
   * @param name the bucket name
   * @return the Couchbase client
   */
  def get(name: String): Try[JsonBucket] = Try {
    if (!buckets.containsKey(name)) {
      val client = new CouchbaseClient(servers, name, password)
      buckets.putIfAbsent(name, new JsonBucket(client))
      Logger.info(s"Creating a new CouchbaseClient for $name...")
    }
    buckets.get(name)
  }

  /**
   * Shuts down all the underlying clients and removes the bucket.
   */
  def shutdown(): Unit = {
    buckets.foreach {
      case (b, c) =>
        c.client.shutdown(60L, TimeUnit.SECONDS)
        Logger.info(s"Shutting down the client to the bucket($b)...")
    }
    buckets.clear()
  }

}
