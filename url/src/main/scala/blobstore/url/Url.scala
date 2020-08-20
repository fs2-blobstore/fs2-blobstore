package blobstore.url

import blobstore.url.exception.{AuthorityParseError, MultipleUrlValidationException, SchemeError, UrlParseError}
import blobstore.url.exception.AuthorityParseError.{InvalidHost, MissingHost}
import blobstore.url.exception.UrlParseError.{CouldntParseUrl, MissingScheme}
import blobstore.url.Authority.StandardAuthority
import blobstore.url.Path.RootlessPath
import cats.{ApplicativeError, Order, Show}
import cats.data.{NonEmptyChain, OptionT, Validated, ValidatedNec}
import cats.data.Validated.{Invalid, Valid}
import cats.instances.either._
import cats.instances.option._
import cats.instances.string._
import cats.instances.try_._
import cats.syntax.all._

import scala.util.Try
import scala.util.matching.Regex

case class Url[A <: Authority] (scheme: String, authority: A, path: Path.Plain)

object Url {
  type PlainUrl = Url[StandardAuthority]

  object PlainUrl {
    def apply(s: String): ValidatedNec[UrlParseError, Url.PlainUrl] = Url.standard(s)
    def unsafe(s: String): Url.PlainUrl = Url.standardUnsafe(s)
  }

  /**
    * RFC 3986, Appendix B.  Parsing a URI Reference with a Regular Expression, https://www.ietf.org/rfc/rfc3986.txt
    *
    * Input string:
    * http://www.ics.uci.edu/pub/ietf/uri/#Related
    *
    * results in the following subexpression matches:
    *
    * $1 = http:
    * $2 = http
    * $3 = //www.ics.uci.edu
    * $4 = www.ics.uci.edu
    * $5 = /pub/ietf/uri/
    * $6 = <undefined>
    * $7 = <undefined>
    * $8 = #Related
    * $9 = Related
    */
  private val regex = "^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?".r

  def apply(s: String): ValidatedNec[UrlParseError, Url.PlainUrl] = standard(s)

  def parse(s: String): ValidatedNec[UrlParseError, Url.PlainUrl] = standard(s)

  // The reason we're using regexes and not just relying on java.net.URL is that URL doesn't handle schemes like 'gs' or 's3'
  def standard(c: String): ValidatedNec[UrlParseError, Url.PlainUrl] = {
    // Treat `m.group` as unsafe, since it really is
    def tryOpt[A](a: => A): Try[Option[A]] = Try(a).map(Option.apply)

    regex.findFirstMatchIn(c).map { m =>
      val authority: Either[AuthorityParseError, String] = OptionT(
        tryOpt(m.group(4)).toEither.leftMap(InvalidHost).leftWiden[AuthorityParseError]
      ).getOrElseF(MissingHost(c).asLeft)

      val typedAuthority: ValidatedNec[AuthorityParseError, StandardAuthority] = authority.leftMap(NonEmptyChain(_)).flatMap(StandardAuthority.parse(_).toEither).toValidated

      val path: Path.Plain = OptionT(tryOpt(m.group(5))).map(Path.apply).getOrElse(Path.empty).getOrElse(Path.empty)
      val scheme =
        OptionT(
          tryOpt(m.group(2)).toEither.leftMap(t => MissingScheme(c, Some(t))).leftWiden[UrlParseError]
        ).getOrElseF(MissingScheme(c, None).asLeft[String]).toValidatedNec

      (scheme, typedAuthority).mapN((s, a) => new Url[StandardAuthority](s, a, path))
    }.getOrElse(CouldntParseUrl(c).invalidNec)
  }

  def standardF[F[_]: ApplicativeError[*[_], Throwable]](c: String): F[Url.PlainUrl] =
    standard(c).leftMap(MultipleUrlValidationException.apply).liftTo[F]

  def standardUnsafe(c: String): Url.PlainUrl = standard(c) match {
    case Valid(u)   => u
    case Invalid(e) => throw MultipleUrlValidationException(e)
  }

  val schemeRegex: Regex = "^[+a-z]+$".r

  def validateScheme[S <: String](candidate: S): Validated[SchemeError, S] =
    Validated.fromOption(schemeRegex.matches(candidate).guard[Option].as(candidate), SchemeError.InvalidScheme(candidate))

  def compare[A <: Authority, P](one: Url[A], two: Url[A]): Int = one.show compare two.show

  implicit def ordering[A <: Authority]: Ordering[Url[A]] = compare
  implicit def order[A <: Authority]: Order[Url[A]] = Order.fromOrdering

  implicit def show[S <: String, A <: Authority]: Show[Url[A]] = u => {
    val pathString = u.path match {
      case r: RootlessPath[_] => show"/$r"
      case a => a.show
    }
    show"${u.scheme}://${u.authority}${pathString}"
  }

}
