package blobstore.url

import blobstore.url.exception.{AuthorityParseError, UrlParseError}
import blobstore.url.exception.AuthorityParseError.{InvalidHost, MissingHost}
import blobstore.url.exception.UrlParseError.{CouldntParseUrl, MissingScheme}
import blobstore.url.Authority.{Bucket, Standard}
import cats.data.{NonEmptyChain, OptionT, ValidatedNec}
import cats.instances.either._
import cats.instances.try_._
import cats.syntax.all._

import scala.util.Try

trait UrlParser[A <: Authority] {

  def parse(s: String): ValidatedNec[UrlParseError, Url[A]]

}

object UrlParser {

  def apply[A <: Authority: UrlParser]: UrlParser[A] = implicitly[UrlParser[A]]

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

  implicit val standardParser: UrlParser[Authority.Standard] = { c =>
    // Treat `m.group` as unsafe, since it really is
    def tryOpt[A](a: => A): Try[Option[A]] = Try(a).map(Option.apply)

    regex.findFirstMatchIn(c).map { m =>
      val authority: Either[AuthorityParseError, String] = OptionT(
        tryOpt(m.group(4)).toEither.leftMap(InvalidHost).leftWiden[AuthorityParseError]
      ).getOrElseF(MissingHost(c).asLeft)

      val typedAuthority: ValidatedNec[AuthorityParseError, Standard] =
        authority.leftMap(NonEmptyChain(_)).flatMap(Standard.parse(_).toEither).toValidated

      val path: Path.Plain = OptionT(tryOpt(m.group(5))).map(Path.apply).getOrElse(Path.empty).getOrElse(Path.empty)
      val scheme =
        OptionT(
          tryOpt(m.group(2)).toEither.leftMap(t => MissingScheme(c, Some(t))).leftWiden[UrlParseError]
        ).getOrElseF(MissingScheme(c, None).asLeft[String]).toValidatedNec

      (scheme, typedAuthority).mapN((s, a) => new Url[Standard](s, a, path))
    }.getOrElse(CouldntParseUrl(c).invalidNec)
  }

  implicit val bucketParser: UrlParser[Authority.Bucket] = standardParser.parse(_).toEither match {
    case Right(u)    => Bucket.parse(u.authority.show).map(a => u.copy(authority = a))
    case Left(error) => error.invalid
  }
}
