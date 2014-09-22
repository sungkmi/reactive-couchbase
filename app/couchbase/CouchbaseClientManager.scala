package couchbase

import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.couchbase.client.CouchbaseClient
import play.api.{Logger, Play}
import play.api.Play.current

import scala.collection.JavaConversions._

trait CouchbaseClientManager {
  /**
   * Holds the Couchbase client.
   */
  val clients = new ConcurrentHashMap[String, CouchbaseClient]()

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
   * Returns a Couchbase client for the given bucket name.
   * @param name the bucket name
   * @return the Couchbase client
   */
  def get(name: String): CouchbaseClient = {
    if (!clients.containsKey(name)) {
      clients.putIfAbsent(name, new CouchbaseClient(servers, name, password))
      Logger.info(s"Creating a new CouchbaseClient for $name...")
    }
    clients.get(name)
  }

  /**
   * Shuts down all the clients.
   */
  def shutdown(): Unit = {
    clients.foreach {
      case (b, c) =>
        c.shutdown(60L, TimeUnit.SECONDS)
        Logger.info(s"Shutting down the client to the bucket($b)...")
    }
    clients.clear()
  }
}
