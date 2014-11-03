package couchbase

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.document.{ Document, JsonLongDocument, JsonStringDocument }
import com.couchbase.client.java.error.{ CASMismatchException, DocumentDoesNotExistException }
import com.couchbase.client.java.view._
import com.couchbase.client.java.{ AsyncBucket, PersistTo, ReplicateTo }
import play.api.libs.json.{ JsValue, Json }
import rx.lang.scala.JavaConversions.toScalaObservable
import rx.lang.scala.Observable

import scala.concurrent.{ Future, Promise }
import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

/**
 * Scala wrapper of the Couchbase java client.
 * @param bucket [[AsyncBucket]]
 */
class AsyncClient(val bucket: AsyncBucket) {

  /**
   * Converts the [[rx.lang.scala.Observable]] to [[scala.concurrent.Future]]
   * @param op the operating which returns the [[rx.lang.scala.Observable]]
   * @tparam A the type of the [[rx.lang.scala.Observable]]
   * @return the future of type [[A]]
   */
  protected def future[A <: Document[_]](op: => Observable[A]): Future[A] = {
    val promise = Promise[A]()
    op.filter(_.id() != null)
      .subscribe(
        n => promise.success(n),
        e => promise.failure(e),
        () => if (!promise.isCompleted) promise.failure(new DocumentDoesNotExistException())
      )
    promise.future
  }

  /**
   * Aggregates the result of the operation which returns [[rx.lang.scala.Observable]]
   * and converts the aggregated result to [[scala.concurrent.Future]].
   * @param docs the sequence of the document which will be processed by the function specified.
   * @param op the function which will process the elements of the document sequence
   * @tparam A the type of the document
   * @return the list of the resulting documents in [[scala.concurrent.Future]]
   *         The list is sorted by the ids of the documents.
   */
  protected def future[A <: Document[_]](
    docs: Seq[A],
    op: A => Observable[A]): Future[List[A]] = {
    val promise = Promise[List[A]]()
    Observable
      .from(docs)
      .flatMap(doc => op(doc))
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
   * Creates a document and returns the newly created document.
   * @param document the document
   * @param persistTo the Couchbase persistence option
   * @param replicateTo the Couchbase replication option
   * @tparam A the type of the document.
   * @return If the document is created successfully, it returns the newly the [[Future]] of the created document.
   *         If there's an error, it returns the failed future.
   */
  def create[A <: Document[_]](
    document: A,
    persistTo: PersistTo = PersistTo.NONE,
    replicateTo: ReplicateTo = ReplicateTo.NONE): Future[A] = {
    future {
      bucket.insert(document, persistTo, replicateTo)
    }
  }

  /**
   * Reads a document.
   * @param document a document instance holding the id of the document to read:
   *                 e.g. JsonDocument.create(id)
   * @tparam A the type of the document.
   * @return If the document is read successfully, it returns the future of the document.
   *         If there's an error, it returns the failed future.
   */
  def read[A <: Document[_]](document: A): Future[A] = {
    future {
      bucket.get(document.id(), document.getClass)
    }
  }

  /**
   * Reads the multiple documents.
   * @param docs the sequence of the document instances holding the id and the type.
   * @tparam A the type of the document
   * @return It returns the list of the documents which are read successfully.
   *         If there's no document found, it returns an empty list.
   */
  def read[A <: Document[_]](docs: Seq[A]): Future[List[A]] = {
    future(docs, (doc: A) => bucket.get[A](doc))
  }

  /**
   * Updates the existing document with the provided document using CAS mechanism.
   * The document to be updated must exist already.
   * @param document the updated document replacing the existing document.
   * @param persistTo the Couchbase persistence option
   * @param replicateTo the Couchbase replication option
   * @tparam A the type of the document.
   * @return If the document is updated successfully, it returns the future of the updated document.
   *         If there's an error, it returns the failed future.
   */
  def update[A <: Document[_]](
    document: A,
    persistTo: PersistTo = PersistTo.NONE,
    replicateTo: ReplicateTo = ReplicateTo.NONE): Future[A] = {
    import scala.concurrent.duration._
    future {
      Observable
        .defer(bucket.get[A](document))
        .flatMap(
          n => bucket.replace(document, persistTo, replicateTo))
        .retryWhen(
          _.flatMap(
            _ match {
              case _: CASMismatchException => Observable.timer(Duration(500, MILLISECONDS))
              case t => Observable.error(t)
            }))
    }
  }

  /**
   * Overwrites the existing document or creates a new document if the document does not exist yet.
   * @param document the document to replace the existing document or the new document to be created.
   * @param persistTo the Couchbase persistence option
   * @param replicateTo the Couchbase replication option
   * @tparam A the type of the document.
   * @return If everything goes well, it returns the future of the document.
   *         If there's an error, it returns the failed future.
   */
  def upsert[A <: Document[_]](
    document: A,
    persistTo: PersistTo = PersistTo.NONE,
    replicateTo: ReplicateTo = ReplicateTo.NONE): Future[A] = {
    future {
      bucket.upsert(document, persistTo, replicateTo)
    }
  }

  /**
   * Overwrites the existing document.
   * @param document the document to replace the existing document
   * @param persistTo the Couchbase persistence option
   * @param replicateTo the Couchbase replication option
   * @tparam A the type of the document.
   * @return If the document is replaces successfully, it returns the future of the updated document.
   *         If the document does not exist or something goes wrong, it returns the failed future.
   */
  def replace[A <: Document[_]](
    document: A,
    persistTo: PersistTo = PersistTo.NONE,
    replicateTo: ReplicateTo = ReplicateTo.NONE): Future[A] = {
    future {
      bucket.replace(document, persistTo, replicateTo)
    }
  }

  /**
   * Deletes the specified documents.
   * @param docs the sequence of documents holding the ids.
   * @tparam A the type of the [[Document]]
   * @return the deleted documents
   */
  def delete[A <: Document[_]](docs: A*) = {
    future(docs, (doc: A) => bucket.remove[A](doc))
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
    val promise = Promise[Long]()
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

  def query[A <: Document[_]: ClassTag](
    design: String,
    view: String,
    viewQuery: ViewQuery,
    target: Class[A])(implicit ev: ClassTag[A]): Future[List[A]] = {
    val promise = Promise[List[A]]
    val observable: Observable[AsyncViewResult] = bucket.query(viewQuery)
    observable
      .flatMap(_.rows())
      .flatMap(_.document(target))
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