package v1.exceptions

case class InvalidIdException(msg: String, cause: Throwable = null) extends Exception(msg, cause)