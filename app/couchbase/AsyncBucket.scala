package couchbase

import com.couchbase.client.CouchbaseClient
import couchbase.CouchbaseExceptions._
import net.spy.memcached.internal._
import net.spy.memcached.{CASResponse, CASValue, PersistTo, ReplicateTo}

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

trait AsyncBucket {

  def client: CouchbaseClient
  /**
   * Transforms the OperationFuture to Scala Future.
   * @param op the function which returns the OperationFuture
   * @tparam U
   * @tparam T
   * @return
   */
  protected def future[U <% T, T](op: => OperationFuture[U]): Future[T] = {
    val promise = Promise[T]
    Try(op) match {
      case Success(f) =>
        f.addListener(new OperationCompletionListener {
          override def onComplete(future: OperationFuture[_]): Unit = {
            Try(future.get().asInstanceOf[T]) match {
              case Success(v) => promise.success(v)
              case Failure(t) => promise.failure(t)
            }
          }
        })
      case Failure(t) => promise.failure(t)
    }
    promise.future
  }

  /**
   * Creates a document asynchronously.
   * If a document exist already with the given key, the call will fail.
   * @param key the key of the document
   * @param value the document
   * @param expiresIn the expiration time of the document. The default value 0 means no expiration.
   * @param persistTo Couchbase persistence option.
   * @param replicateTo Couchbase replication option.
   * @return the created document
   */
  def create(key: String,
             value: String,
             expiresIn: Int = 0,
             persistTo: PersistTo = PersistTo.ZERO,
             replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[String] = {
    future[java.lang.Boolean, Boolean] {
      client.add(key, expiresIn, value, persistTo, replicateTo)
    } flatMap {
      case true => Future.successful(value)
      case false => Future.failed(CreateException(new Exception(s"Failed to create: $key")))
    } recoverWith {
      case t: Throwable => Future.failed(CreateException(t))
    }
  }

  /**
   * Updates a document asynchronously.
   * If a document does not exist yet with the given key, the call will fail.
   * @param key the key of the document
   * @param value the updated document
   * @param expiresIn the expiration time of the document. The default value 0 means no expiration.
   * @param persistTo Couchbase persistence option.
   * @param replicateTo Couchbase replication option.
   * @return the updated document
   */
  def update(key: String,
             value: String,
             expiresIn: Int = 0,
             persistTo: PersistTo = PersistTo.ZERO,
             replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[String] = {
    future[java.lang.Boolean, Boolean] {
      client.replace(key, expiresIn, value, persistTo, replicateTo)
    } flatMap {
      case true => Future.successful(value)
      case false => Future.failed(UpdateException(new Exception(s"Failed to update: $key")))
    } recoverWith {
      case t: Throwable => Future.failed(UpdateException(t))
    }
  }

  /**
   * Reads a document asynchronously.
   * @param key the key of the document
   * @return the document or an exception if the document does not exist or an error occurs.
   */
  def read(key: String)(implicit ec: ExecutionContext): Future[String] = {
    val promise = Promise[String]
    Try(client.asyncGet(key)) match {
      case Success(gf) =>
        gf.addListener(new GetCompletionListener {
          override def onComplete(future: GetFuture[_]): Unit = {
            Try(future.get().asInstanceOf[String]) match {
              case Success(d) if d != null => promise.success(d)
              case Success(_) => promise.failure(ReadException(new Exception(s"Not found: $key")))
              case Failure(t) => promise.failure(ReadException(t))
            }
          }
        })
      case Failure(t) => promise.failure(ReadException(t))
    }
    promise.future
  }

  /**
   * Reads multiple documents specified by the keys asynchronously.
   * @param keys the keys of the documents
   * @return a map of the keys and the documents.
   */
  def readBulk(keys: Iterable[String]): Future[Map[String, String]] = {
    import scala.collection.JavaConversions._
    val promise = Promise[Map[String, String]]
    Try(client.asyncGetBulk(keys)) match {
      case Success(f) =>
        f.addListener(new BulkGetCompletionListener {
          def onComplete(future: BulkGetFuture[_]) {
            Try(future.get().asInstanceOf[java.util.Map[String, Any]].toSeq) match {
              case Success(map) =>
                val results =
                  for {
                    (k, v) <- map
                    d <- Try(v.asInstanceOf[String]).toOption
                  } yield {
                    (k, d)
                  }
                promise.success(SortedMap(results: _*))
              case Failure(t) => promise.failure(ReadBulkException(t))
            }
          }
        })
      case Failure(t) => promise.failure(ReadBulkException(t))
    }
    promise.future
  }

  /**
   * Reads the document with the CAS value asynchronously.
   * @param key the key of the document
   * @return a tuple of the CAS value and the document
   */
  def readCAS(key: String)(implicit ec: ExecutionContext): Future[(Long, String)] = {
    import scala.language.implicitConversions

    implicit def casConvert(cas: CASValue[java.lang.Object]): CASValue[String] = {
      new CASValue(cas.getCas, cas.getValue.asInstanceOf[String])
    }

    future[CASValue[java.lang.Object], CASValue[String]] {
      client.asyncGets(key)
    } flatMap {
      cas =>
        Try {
          (cas.getCas, cas.getValue)
        } match {
          case Success(r) => Future.successful(r)
          case Failure(t) => Future.failed(CASReadException(t))
        }
    } recoverWith {
      case t: Throwable => Future.failed(CASReadException(t))
    }
  }

  /**
   * Writes the document with the CAS value asynchronously.
   * @param key the key of the document
   * @param value the updated document
   * @param cas the CAS value
   * @param expiresIn the expiration time of the document. The default value 0 means no expiration.
   * @return the updated document if writing is successful.
   */
  def writeCAS(key: String,
               value: String,
               cas: Long,
               expiresIn: Int = 0)(implicit ec: ExecutionContext): Future[String] = {
    future[CASResponse, CASResponse] {
      client.asyncCAS(key, cas, expiresIn, value)
    } flatMap {
      case CASResponse.OK => Future.successful(value)
      case _ => Future.failed(CASWriteException(new Exception(s"Failed to write with CAS value: $key")))
    } recoverWith {
      case t: Throwable => Future.failed(t)
    }
  }

  /**
   * Deletes the document asynchronously.
   * @param key the key of the document.
   * @return true if the document is deleted successfully. Otherwise, it throws an exception.
   */
  def delete(key: String,
             persistTo: PersistTo = PersistTo.ZERO,
             replicateTo: ReplicateTo = ReplicateTo.ZERO)(implicit ec: ExecutionContext): Future[Boolean] = {
    future[java.lang.Boolean, Boolean] {
      client.delete(key, persistTo, replicateTo)
    } flatMap {
      case true => Future.successful(true)
      case false => Future.failed(DeleteException(new Exception(s"Failed to delete: $key")))
    } recoverWith {
      case t: Throwable => Future.failed(DeleteException(t))
    }
  }

  /**
   * Increases the counter.
   * @param key the key of the counter
   * @param by the increment
   * @param default the default value of the counter, if the counter is not initialized.
   * @param expiresIn the expiration of the counter
   * @return the counter value
   */
  def incr(key: String,
           by: Long = 1L,
           default: Long = 1L,
           expiresIn: Int = 0)(implicit ec: ExecutionContext): Future[Long] = {
    future[java.lang.Long, Long] {
      client.asyncIncr(key, by, default, expiresIn)
    } recoverWith {
      case t: Throwable => Future.failed(CounterException(t))
    }
  }

  /**
   * Decreases the counter.
   * @param key the key of the counter
   * @param by the decrement
   * @param default the default value of the counter, if the counter is not initialized.
   * @param expiresIn the expiration of the counter
   * @return the counter value
   */
  def decr(key: String,
           by: Long = 1L,
           default: Long = 1L,
           expiresIn: Int = 0)(implicit ec: ExecutionContext): Future[Long] = {
    future[java.lang.Long, Long] {
      client.asyncDecr(key, by, default, expiresIn)
    } recoverWith {
      case t: Throwable => Future.failed(CounterException(t))
    }
  }
}
