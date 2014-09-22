package couchbase

import com.couchbase.client.internal.{HttpCompletionListener, HttpFuture}
import com.couchbase.client.protocol.views.{Query, View, ViewResponse}
import couchbase.CouchbaseExceptions.{CASReadException, ReadException}
import net.spy.memcached.{PersistTo, ReplicateTo}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

trait JsonClient extends AsyncClient {

  def createJson(key: String,
                 json: JsValue,
                 expiresIn: Int = 0,
                 persistTo: PersistTo = PersistTo.ZERO,
                 replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[JsValue] = {
    create(key, Json.stringify(json), expiresIn, persistTo, replicateTo) map (_ => json)
  }

  def updateJson(key: String,
                 json: JsValue,
                 expiresIn: Int = 0,
                 persistTo: PersistTo = PersistTo.ZERO,
                 replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[JsValue] = {
    update(key, Json.stringify(json), expiresIn, persistTo, replicateTo) map (_ => json)
  }

  def readJson(key: String)(implicit ec: ExecutionContext): Future[JsValue] = {
    read(key) flatMap {
      doc =>
        Try(Json.parse(doc)) match {
          case Success(json) => Future.successful(json)
          case Failure(t) => Future.failed(ReadException(t))
        }
    }
  }

  def readJsonBulk(keys: Iterable[String])(implicit ec: ExecutionContext): Future[Map[String, JsValue]] = {
    readBulk(keys) map {
      map =>
        for {
          (k, d) <- map
          j <- Try(Json.parse(d)).toOption
        } yield {
          (k, j)
        }
    }
  }

  def readJsonCAS(key: String)(implicit ec: ExecutionContext): Future[(Long, JsValue)] = {
    readCAS(key) flatMap {
      case (c, v) =>
        Try(Json.parse(v.asInstanceOf[String])) match {
          case Success(json) => Future.successful((c, json))
          case Failure(t) => Future.failed(CASReadException(t))
        }
    }
  }

  def writeJsonCAS(key: String,
                   json: JsValue,
                   cas: Long,
                   expiresIn: Int = 0)(implicit ec: ExecutionContext) = {
    writeCAS(key, Json.stringify(json), cas, expiresIn) map (_ => json)
  }

  def queryJson[T](view: View, query: Query): Future[Map[String, JsValue]] = {
    import scala.collection.JavaConversions._
    val promise = Promise[Map[String, JsValue]]
    Try(client.asyncQuery(view, query)) match {
      case Success(httpFuture) =>
        httpFuture.addListener(new HttpCompletionListener {
          def onComplete(future: HttpFuture[_]) {
            Try(future.get().asInstanceOf[ViewResponse]) match {
              case Success(vr) =>
                val results =
                  (for {
                    row <- vr
                    key = row.getKey
                    json <- Try(Json.parse(row.getDocument.asInstanceOf[String])).toOption
                  } yield {
                    (key, json)
                  }).toMap
                promise.success(results)
              case Failure(t) => promise.failure(t)
            }
          }
        })
      case Failure(t) => promise.failure(t)
    }
    promise.future
  }

}
