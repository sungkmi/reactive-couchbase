package couchbase

import play.api.libs.json.Json

case class BucketInfo(name: String, password: String = "")

object BucketInfo {
  implicit val format = Json.format[BucketInfo]
}