package blobstore.experiment.url

import blobstore.experiment.exception.{AuthorityParseError, MultipleUrlValidationException, SchemeError, SingleValidationException, UrlParseError}
import blobstore.experiment.exception.AuthorityParseError.{InvalidHost, MissingHost}
import blobstore.experiment.exception.UrlParseError.{CouldntParseUrl, MissingScheme}
import blobstore.experiment.url.Authority.{Bucket, StandardAuthority}
import blobstore.experiment.url.Path.{EmptyPath, RootlessPath}
import cats.{ApplicativeError, Show}
import cats.data.{NonEmptyChain, OptionT, Validated, ValidatedNec}
import cats.data.Validated.{Invalid, Valid}
import cats.instances.either._
import cats.instances.option._
import cats.instances.string._
import cats.instances.try_._
import cats.syntax.all._
import shapeless.Witness
import com.github.ghik.silencer.silent

import scala.util.Try
import scala.util.matching.Regex

class Url[+S <: String, A <: Authority, P] private (val scheme: S, val authority: A, val path: Path[P])  extends Product3[S, A, Path[P]] with Serializable {
//  def narrowBucket: Validated[BucketParseError.NotValidBucketUrl, Url.Bucket] = authority match {
//    case bucket@Bucket(_ ,_) => new Url[Bucket](scheme, bucket, path).valid
//    case a: StandardAuthority => BucketParseError.NotValidBucketUrl(new Url[StandardAuthority](scheme, a, path)).invalid
//  }
  override def _1: S = scheme

  override def _2: A = authority

  override def _3: Path[P] = path

  @silent("unchecked")
  override def canEqual(that: Any): Boolean =
    that.isInstanceOf[Url[String, Authority, Any]]

  def withScheme[SS <: String](newScheme: SS): Validated[SchemeError, Url[SS, A, P]] =
    Url.validateScheme(newScheme).map(s => new Url(s, authority, path))

  def unsafeWithScheme[SS <: String](newScheme: SS): Url[SS, A, P] = withScheme(newScheme) match {
    case Valid(a) => a
    case Invalid(e) => throw SingleValidationException(e)
  }

  def withAuthority[AA <: Authority](newAuthority: AA): ValidatedNec[AuthorityParseError, AA] = ???
  def unusafeWithAuthority[AA <: Authority](newAuthority: AA): AA = ???

  def withPath[PP](newPath: Path[PP]): Url[S, A, Path[PP]] = ???
}

object Url {

  object scheme {
    type Http = Witness.`"http"`.T
    type Https = Witness.`"https"`.T
  }


  type Http = Url[scheme.Http, StandardAuthority, String]
  type Https = Url[scheme.Https, StandardAuthority, String]
  type Standard = Url[String, StandardAuthority, String]

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

  def apply(s: String): ValidatedNec[UrlParseError, Url.Standard] = standard(s)

  def parse(s: String): ValidatedNec[UrlParseError, Url.Standard] = standard(s)

  // The reason we're using regexes and not just relying on java.net.URL is that URL doesn't handle schemes like 'gs' or 's3'
  def standard(c: String): ValidatedNec[UrlParseError, Url.Standard] = {
    // Treat `m.group` as unsafe, since it really is
    def tryOpt[A](a: => A): Try[Option[A]] = Try(a).map(Option.apply)

    regex.findFirstMatchIn(c).map { m =>
      val authority: Either[AuthorityParseError, String] = OptionT(
        tryOpt(m.group(4)).toEither.leftMap(InvalidHost).leftWiden[AuthorityParseError]
      ).getOrElseF(MissingHost(c).asLeft)

      val typedAuthority: ValidatedNec[AuthorityParseError, StandardAuthority] = authority.leftMap(NonEmptyChain(_)).flatMap(StandardAuthority.parse(_).toEither).toValidated

      val path: Path.Plain = OptionT(tryOpt(m.group(5))).map(Path.apply).getOrElse(EmptyPath).getOrElse(EmptyPath)
      val scheme =
        OptionT(
          tryOpt(m.group(2)).toEither.leftMap(t => MissingScheme(c, Some(t))).leftWiden[UrlParseError]
        ).getOrElseF(MissingScheme(c, None).asLeft[String]).toValidatedNec

      (scheme, typedAuthority).mapN((s, a) => new Url[String, StandardAuthority, String](s, a, path))
    }.getOrElse(CouldntParseUrl(c).invalidNec)
  }

  def standardF[F[_]: ApplicativeError[*[_], Throwable]](c: String): F[Url.Standard] =
    standard(c).leftMap(MultipleUrlValidationException.apply).liftTo[F]

  def standardUnsafe(c: String): Url.Standard = standard(c) match {
    case Valid(u)   => u
    case Invalid(e) => throw MultipleUrlValidationException(e)
  }

  val schemeRegex: Regex = "^[+a-z]+$".r

  def validateScheme[S <: String](candidate: S): Validated[SchemeError, S] =
    Validated.fromOption(schemeRegex.matches(candidate).guard[Option].as(candidate), SchemeError.InvalidScheme(candidate))

  def compare[A <: Authority, P](one: Url[String, A, P], two: Url[String, A, P]): Int = {
    val scheme = one.scheme compare two.scheme
    val authority = (one.authority, two.authority) match {
      case (StandardAuthority(host1, userInfo1, port1), StandardAuthority(host2, userInfo2, port2)) =>
        List(host1 compare host2, userInfo1 compare userInfo2, port1 compare port2).find(_ != 0).getOrElse(0)
      case (Bucket(h1, _), Bucket(h2, _)) => h1 compare h2
    }
    val path = Path.compare(one.path, two.path)

    List(scheme, authority, path).find(_ != 0).getOrElse(0)
  }

//  implicit def ordering[A <: Authority]: Ordering[Url[A]] = compare
//  implicit def order[A <: Authority]: Order[Url[A]] = Order.fromOrdering

  implicit def show[S <: String, A <: Authority, P]: Show[Url[S, A, P]] = u => {
    val pathString = u.path match {
      case r: RootlessPath[P] => show"/${r.show}"
      case a => a.show
    }
    show"${u.scheme}://${u.authority}${pathString}"
  }

}
