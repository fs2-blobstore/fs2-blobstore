package blobstore.url.exception

case class SingleValidationException(error: UrlParseError, cause: Option[Throwable] = None)
  extends Exception(error.error, cause.orNull)
