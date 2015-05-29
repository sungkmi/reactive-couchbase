package couchbase

import play.api.libs.json.Json

case class BucketInfo(name: String, password: String = "")

object BucketInfo {
  implicit val format = Json.format[BucketInfo]
}

class NoBucketInformation(name: String)
  extends Exception(s"No access information for the $name bucket.")