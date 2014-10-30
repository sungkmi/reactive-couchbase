package couchbase

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.document.{ JsonLongDocument, JsonStringDocument }
import com.couchbase.client.java.error.{ CASMismatchException, DocumentDoesNotExistException }
import com.couchbase.client.java.view._
import com.couchbase.client.java.{ AsyncBucket, PersistTo, ReplicateTo }
import play.api.libs.json.{ JsValue, Json }
import rx.lang.scala.JavaConversions.toScalaObservable
import rx.lang.scala.Observable

import scala.concurrent.{ Future, Promise }
import scala.util.Try

class AsyncClient(val bucket: AsyncBucket) {

  /**
   * Converts the RxScala Observable to Future.
   * @param op the operation returning the RxScala Observable.
   * @return the Future
   */
  protected def future(op: => Observable[JsonStringDocument]): Future[JsonStringDocument] = {
    val promise = Promise[JsonStringDocument]
    op
      .orElse(JsonStringDocument.empty())
      .subscribe(
        n => {
          if (n.id() != null)
            promise.success(n)
          else
            promise.failure(new DocumentDoesNotExistException())
        },
        e => promise.failure(e)
      )
    promise.future
  }

  /**
   * Converts the list of Observables to a Future holding the list of the results.
   * @param ids the list of document ids'
   * @param op the operation
   * @return
   */
  protected def future(ids: Array[String],
    op: String => Observable[JsonStringDocument]): Future[List[JsonStringDocument]] = {
    val promise = Promise[List[JsonStringDocument]]
    Observable
      .from(ids)
      .flatMap(id => op(id))
      .retry
      .toList
      .subscribe(
        n => promise.success(n.sortBy(_.id())),
        e => promise.failure(e),
        () => if (!promise.isCompleted) promise.success(Nil)
      )
    promise.future
  }

  /**
   * Creates a document asynchronously.
   * If a document exist already with the given key, the call will fail.
   * @param id the key of the document
   * @param content the document content
   * @param expiry the expiration time of the document. The default value 0 means no expiration.
   * @param persistTo Couchbase persistence option.
   * @param replicateTo Couchbase replication option.
   * @return the created document
   */
  def create(
    id: String,
    content: String,
    expiry: Int = 0,
    persistTo: PersistTo = PersistTo.NONE,
    replicateTo: ReplicateTo = ReplicateTo.NONE): Future[JsonStringDocument] = {
    val doc = JsonStringDocument.create(id, content, expiry)
    future {
      bucket.insert(doc, persistTo, replicateTo)
    }
  }

  /**
   * Reads the specified document.
   * @param id the document id
   * @return If the document exist for the id, it returns [[JsonStringDocument]].
   *         If the document does not exist for the id,
   *         it returns [[com.couchbase.client.java.error.DocumentDoesNotExistException]].
   */
  def read(id: String): Future[JsonStringDocument] = {
    future {
      bucket.get(id, classOf[JsonStringDocument])
    }
  }

  /**
   * Reads the specified documents.
   * @param ids the sequence of ids
   * @return the list of the JsonStringDocument with the specified ids.
   *         The non-existent document is not included in the list.
   */
  def read(ids: Seq[String]): Future[List[JsonStringDocument]] = {
    future(ids.toSet.toArray, bucket.get(_, classOf[JsonStringDocument]))
  }

  /**
   * Updates a document asynchronously.
   * If a document does not exist yet with the given key, the call will fail.
   * @param id the document id
   * @param content the updated content of the document
   * @param expiry the expiration time of the document. The default value 0 means no expiration.
   * @param persistTo Couchbase persistence option
   * @param replicateTo Couchbase replication option
   * @return the updated document
   */
  def update(
    id: String,
    expiry: Int = 0,
    persistTo: PersistTo = PersistTo.NONE,
    replicateTo: ReplicateTo = ReplicateTo.NONE)(update: String => String): Future[JsonStringDocument] = {
    import scala.concurrent.duration._
    future {
      Observable.defer(bucket.get(id, classOf[JsonStringDocument]))
        .flatMap(
          n => {
            val doc = JsonStringDocument.create(n.id(), expiry, update(n.content()), n.cas())
            bucket.replace(doc, persistTo, replicateTo)
          })
        .retryWhen(
          _.flatMap(
            _ match {
              case _: CASMismatchException => Observable.timer(Duration(500, MILLISECONDS))
              case t => Observable.error(t)
            }))
    }
  }

  /**
   * Updates the existing document or creates a new document if the document does not exist at the given id.
   * @param id the document id
   * @param content the document content
   * @param expiry the expiration time of the document. The default value 0 means no expiration.
   * @param persistTo Couchbase persistence option
   * @param replicateTo Couchbase replication option
   * @return the updated document or the created document if the document did not exist before.
   */
  def upsert(
    id: String,
    content: String,
    expiry: Int = 0,
    persistTo: PersistTo = PersistTo.NONE,
    replicateTo: ReplicateTo = ReplicateTo.NONE) = {
    future {
      val doc = JsonStringDocument.create(id, expiry, content)
      bucket.upsert(doc, persistTo, replicateTo)
    }
  }

  /**
   * Replaces the existing document with the given content.
   * @param id the document id
   * @param content the new content
   * @param expiry the expiration time of the document. The default value 0 means no expiration.
   * @param persistTo Couchbase persistence option
   * @param replicateTo Couchbase replication option
   * @return the updated document
   */
  def replace(
    id: String,
    content: String,
    expiry: Int = 0,
    persistTo: PersistTo = PersistTo.NONE,
    replicateTo: ReplicateTo = ReplicateTo.NONE): Future[JsonStringDocument] = {
    future {
      val doc = JsonStringDocument.create(id, expiry, content)
      bucket.replace(doc, persistTo, replicateTo)
    }
  }

  /**
   * Deletes the specified documents.
   * @param ids the document ids
   * @return the list of the deleted Document
   */
  def delete(ids: String*): Future[List[JsonStringDocument]] = {
    future(ids.toArray, bucket.remove(_, classOf[JsonStringDocument]))
  }

  /**
   * Increments or decrements the counter with the given delta and the initial value.
   * @param id the counter id
   * @param delta the delta value to change the counter
   * @param initial the initial counter value if the counter document does not exists in the bucket
   * @param expiry the expiry of the counter document
   * @return
   */
  def counter(
    id: String,
    delta: Long = 1,
    initial: Long = 1,
    expiry: Int = 0): Future[Long] = {
    val promise = Promise[Long]
    val observable: Observable[JsonLongDocument] = bucket.counter(id, delta, initial, expiry)
    observable
      .subscribe(
        n => promise.success(n.content()),
        e => promise.failure(e),
        () =>
          if (!promise.isCompleted)
            promise.failure(new CouchbaseException("Failed to access the counter."))

      )
    promise.future
  }

  def query(
    design: String,
    view: String,
    viewQuery: ViewQuery): Future[List[JsonStringDocument]] = {
    val promise = Promise[List[JsonStringDocument]]
    val observable: Observable[AsyncViewResult] = bucket.query(viewQuery)
    observable
      .flatMap(_.rows())
      .flatMap(_.document(classOf[JsonStringDocument]))
      .toList
      .subscribe(
        n => promise.success(n),
        e => promise.failure(e),
        () =>
          if (!promise.isCompleted) promise.success(Nil)
      )
    promise.future
  }

}

object AsyncClient {

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

    def toFuture: Future[JsValue] = Future.fromTry(Try(Json.parse(from.content())))
  }

  implicit def doc2RichDoc(from: JsonStringDocument) = new RichJsonStringDocument(from)

}

