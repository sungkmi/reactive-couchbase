package couchbase

import javax.inject.{ Inject, Singleton }

import play.api.Application

/**
 * Manages the Couchbase bucket access information.
 */
trait BucketInfoManager {
  /**
   * Returns the bucket access information for the name.
   * @param name the name of the bucket
   * @return
   */
  def get(name: String): Option[BucketInfo]

  def gets(): Map[String, BucketInfo]

  val bucketsKey = "couchbase.buckets"
}

@Singleton
class BucketInfoManagerImpl @Inject() (application: Application) extends BucketInfoManager {

  protected val bis: Map[String, BucketInfo] = config()

  /**
   * Returns the bucket name and password.
   * @param name the name of the bucket
   * @return
   */
  def get(name: String): Option[BucketInfo] = {
    bis.get(name)
  }

  def gets() = bis

  protected def config(): Map[String, BucketInfo] = {
    import scala.collection.JavaConversions._
    application.configuration.getConfigList(bucketsKey).map { cfgs =>
      (for {
        cfg <- cfgs
        name <- cfg.getString("name")
        password <- cfg.getString("password")
      } yield {
        name -> BucketInfo(name, password)
      }).toMap
    } getOrElse Map[String, BucketInfo]()
  }
}
