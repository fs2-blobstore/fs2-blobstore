package blobstore.url.exception

import cats.data.NonEmptyChain
import cats.syntax.all.*

case class MultipleUrlValidationException(errors: NonEmptyChain[UrlParseError])
  extends Exception(
    errors.map(_.error).toList.mkString("Multiple validation errors: ", "\n", ""),
    errors.reduce.cause.orNull
  )
