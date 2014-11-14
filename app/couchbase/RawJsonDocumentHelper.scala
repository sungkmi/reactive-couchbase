package couchbase

import com.couchbase.client.java.document.RawJsonDocument
import play.api.libs.json.{ JsValue, Json }

import scala.concurrent.{ Future, Promise }
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

object RawJsonDocumentHelper {

  /**
   * Helper class to facilitate the conversion from JsonStringDocument to Scala values.
   * @param from RawJsonDocument
   */
  class RichJsonStringDocument(val from: RawJsonDocument) {
    def toKV: Try[(String, JsValue)] = Try {
      (from.id(), Json.parse(from.content()))
    }

    def toJson: JsValue = Json.parse(from.content())

    def toTry: Try[JsValue] = Try(Json.parse(from.content()))

    def toFuture: Future[JsValue] = {
      val promise = Promise[JsValue]
      Try(Json.parse(from.content())) match {
        case Success(json) => promise.success(json)
        case Failure(t) => promise.failure(t)
      }
      promise.future
    }
  }

  implicit def doc2RichDoc(from: RawJsonDocument) = new RichJsonStringDocument(from)

}

