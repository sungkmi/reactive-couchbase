package couchbase

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.document.{ Document, JsonDocument, JsonLongDocument }
import com.couchbase.client.java.error.{ CASMismatchException, DocumentDoesNotExistException }
import com.couchbase.client.java.view._
import com.couchbase.client.java.{ AsyncBucket, PersistTo, ReplicateTo }
import rx.lang.scala.JavaConversions.toScalaObservable
import rx.lang.scala.Observable

import scala.concurrent.{ Future, Promise }
import scala.reflect.ClassTag

/**
 * Scala wrapper of the Couchbase java client.
 * @param bucket [[com.couchbase.client.java.AsyncBucket]]
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
   * Creates a document and returns the newly created document.
   * @param document the document
   * @param persistTo the Couchbase persistence option
   * @param replicateTo the Couchbase replication option
   * @tparam A the type of the document
   * @return If the document is created successfully, it returns the newly the [[scala.concurrent.Future]] of the created document.
   *         If there's an error, it returns the failed future.
   */
  def create[A <: Document[_]](
    document: A,
    persistTo: PersistTo,
    replicateTo: ReplicateTo): Future[A] = {
    future {
      bucket.insert(document, persistTo, replicateTo)
    }
  }

  /**
   * Creates a document and returns the newly created document.
   * @param document the document
   * @tparam A the type of the document
   * @return If the document is created successfully, it returns the newly the [[scala.concurrent.Future]] of the created document.
   *         If there's an error, it returns the failed future.
   */
  def create[A <: Document[_]](document: A): Future[A] = {
    future {
      bucket.insert(document)
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
  def read[A <: Document[_]](docs: Seq[A]): Future[Seq[A]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val results: Seq[Future[Option[A]]] = docs map { d =>
      read(d)
        .map(Some(_))
        .recover {
          case _: DocumentDoesNotExistException => None
        }
    }
    Future.sequence(results) map (_.filter(_.isDefined).map(_.get))
  }

  /**
   * Updates the existing document with the provided document using CAS mechanism.
   * The document to be updated must exist already.
   * @param document the updated document replacing the existing document.
   * @param persistTo the Couchbase persistence option
   * @param replicateTo the Couchbase replication option
   * @tparam A the type of the document
   * @return If the document is updated successfully, it returns the future of the updated document.
   *         If there's an error, it returns the failed future.
   */
  def update[A <: Document[_]](
    document: A,
    persistTo: PersistTo,
    replicateTo: ReplicateTo): Future[A] = {
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
   * Updates the existing document with the provided document using CAS mechanism.
   * The document to be updated must exist already.
   * @param document the updated document replacing the existing document.
   * @tparam A the type of the document
   * @return If the document is updated successfully, it returns the future of the updated document.
   *         If there's an error, it returns the failed future.
   */
  def update[A <: Document[_]](document: A): Future[A] = {
    import scala.concurrent.duration._
    future {
      Observable
        .defer(bucket.get[A](document))
        .flatMap(
          n => bucket.replace(document))
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
   * @tparam A the type of the document
   * @return If everything goes well, it returns the future of the document.
   *         If there's an error, it returns the failed future.
   */
  def upsert[A <: Document[_]](
    document: A,
    persistTo: PersistTo,
    replicateTo: ReplicateTo): Future[A] = {
    future {
      bucket.upsert(document, persistTo, replicateTo)
    }
  }

  /**
   * Overwrites the existing document or creates a new document if the document does not exist yet.
   * @param document the document to replace the existing document or the new document to be created.
   * @tparam A the type of the document
   * @return If everything goes well, it returns the future of the document.
   *         If there's an error, it returns the failed future.
   */
  def upsert[A <: Document[_]](document: A): Future[A] = {
    future {
      bucket.upsert(document)
    }
  }

  /**
   * Overwrites the existing document.
   * @param document the document to replace the existing document
   * @param persistTo the Couchbase persistence option
   * @param replicateTo the Couchbase replication option
   * @tparam A the type of the document
   * @return If the document is replaces successfully, it returns the future of the updated document.
   *         If the document does not exist or something goes wrong, it returns the failed future.
   */
  def replace[A <: Document[_]](
    document: A,
    persistTo: PersistTo,
    replicateTo: ReplicateTo): Future[A] = {
    future {
      bucket.replace(document, persistTo, replicateTo)
    }
  }

  /**
   * Overwrites the existing document.
   * @param document the document to replace the existing document
   * @tparam A the type of the document
   * @return If the document is replaces successfully, it returns the future of the updated document.
   *         If the document does not exist or something goes wrong, it returns the failed future.
   */
  def replace[A <: Document[_]](document: A): Future[A] = {
    future {
      bucket.replace(document)
    }
  }

  /**
   * Deletes the specified documents.
   * @param id the ID of the document to be deleted
   * @return the [[JsonDocument]] which contains the id and cas value of the deleted document
   */
  def delete(id: String): Future[JsonDocument] = {
    future(bucket.remove(id))
  }

  def delete(ids: Seq[String]): Future[Seq[JsonDocument]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val results = ids map { id =>
      delete(id)
        .map(Some(_))
        .recover {
          case t: DocumentDoesNotExistException => None
        }
    }
    Future.sequence(results).map(_.filter(_.isDefined).map(_.get))
  }

  /**
   * Renews the expiration of the document.
   * @param doc the document holding the id of the document to be renewed.
   * @tparam A the type of the document
   * @return true is the touch was successful otherwise false
   */
  def touch[A <: Document[_]](doc: A) = {
    val promise = Promise[Boolean]
    val observable: Observable[java.lang.Boolean] = bucket.touch(doc)
    observable.subscribe(
      promise.success(_),
      promise.failure(_)
    )
    promise.future
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

  /**
   * Returns the current value of the counter specified by the ID.
   * @param id the counter ID
   * @return the current counter value
   */
  def readCounter(id: String): Future[Long] = {
    counter(id, 0)
  }

  /**
   * Returns the query results.
   * @param design the design document name
   * @param view the view name
   * @param viewQuery the query to be executed
   * @param target the class type of the [[Document]]'s subclass
   * @param tag implicit class tag
   * @tparam A the subclass of the [[Document]]
   * @return
   */
  def query[A <: Document[_]](
    design: String,
    view: String,
    viewQuery: ViewQuery,
    target: Class[A])(implicit tag: ClassTag[A]): Future[List[A]] = {
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