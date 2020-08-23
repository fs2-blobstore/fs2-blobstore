package blobstore.url

import blobstore.url.exception.{MultipleUrlValidationException, UrlParseError}
import blobstore.url.Authority.{Bucket, Standard}
import blobstore.url.Path.AbsolutePath
import cats.{ApplicativeError, Order, Show}
import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNec
import cats.instances.string._
import cats.syntax.all._

case class Url[+A <: Authority](scheme: String, authority: A, path: Path.Plain)

object Url {

  type Plain = Url[Standard]

  def apply(s: String): ValidatedNec[UrlParseError, Url[Standard]] = parse[Authority.Standard](s)

  def forBucket(s: String): ValidatedNec[UrlParseError, Url[Bucket]] = parse[Authority.Bucket](s)

  def parse[A <: Authority: UrlParser](s: String): ValidatedNec[UrlParseError, Url[A]] = UrlParser[A].parse(s)

  def parseF[F[_]: ApplicativeError[*[_], Throwable], A <: Authority: UrlParser](c: String): F[Url[A]] =
    parse[A](c).leftMap(MultipleUrlValidationException.apply).liftTo[F]

  def unsafe[A <: Authority: UrlParser](c: String): Url[A] = parse[A](c) match {
    case Valid(u)   => u
    case Invalid(e) => throw MultipleUrlValidationException(e)
  }

  implicit def ordering[A <: Authority]: Ordering[Url[A]] = _.show compare _.show
  implicit def order[A <: Authority]: Order[Url[A]] = Order.fromOrdering
  implicit def show[S <: String, A <: Authority]: Show[Url[A]] = u => {
    val pathString = u.path match {
      case a@AbsolutePath(_, _) => a.show.stripSuffix("/")
      case a => a.show
    }
    show"${u.scheme}://${u.authority}/$pathString"
  }

}
