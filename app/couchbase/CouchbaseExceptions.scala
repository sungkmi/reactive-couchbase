package couchbase


object CouchbaseExceptions {

  abstract class CouchbaseException(cause: Throwable) extends Exception(cause)

  case class ReadException(cause: Throwable) extends CouchbaseException(cause)

  case class ReadBulkException(cause: Throwable) extends CouchbaseException(cause)

  case class CreateException(cause: Throwable) extends CouchbaseException(cause)

  case class UpdateException(cause: Throwable) extends CouchbaseException(cause)

  case class CASReadException(cause: Throwable) extends CouchbaseException(cause)

  case class CASWriteException(cause: Throwable) extends CouchbaseException(cause)

  case class DeleteException(cause: Throwable) extends CouchbaseException(cause)

  case class QueryException(cause: Throwable) extends CouchbaseException(cause)

  case class CounterException(cause: Throwable) extends CouchbaseException(cause)

}
