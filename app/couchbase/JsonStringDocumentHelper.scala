package couchbase

import com.couchbase.client.java.document.JsonStringDocument
import play.api.libs.json.{ Json, JsValue }

import scala.concurrent.{ Promise, Future }
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

object JsonStringDocumentHelper {

  /**
   * Helper class to facilitate the conversion from JsonStringDocument to Scala values.
   * @param from JsonStringDocument
   */
  class RichJsonStringDocument(val from: JsonStringDocument) {
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

  implicit def doc2RichDoc(from: JsonStringDocument) = new RichJsonStringDocument(from)

}

