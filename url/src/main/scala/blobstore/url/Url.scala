package blobstore.url

import blobstore.url.exception.{AuthorityParseError, MultipleUrlValidationException, UrlParseError}
import blobstore.url.Path.AbsolutePath
import blobstore.url.exception.AuthorityParseError.{InvalidFileUrl, InvalidHost, MissingHost}
import blobstore.url.exception.UrlParseError.{CouldntParseUrl, MissingScheme}
import cats.{ApplicativeError, Order, Show}
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyChain, OptionT, ValidatedNec}
import cats.syntax.all._

import scala.util.Try

case class Url[+A](scheme: String, authority: Authority, path: Path[A]) {
  lazy val representation: A = path.representation

  def plain: Url.Plain = copy(path = path.plain)

  def withPath[AA](p: Path[AA]): Url[AA]     = copy(path = p)
  def withAuthority(a: Authority): Url.Plain = copy(authority = a, path = path.plain)

  def toS3(bucketName: Hostname): Url.Plain    = copy(scheme = "s3", authority = Authority(bucketName), path.plain)
  def toGcs(bucketName: Hostname): Url.Plain   = copy(scheme = "gs", authority = Authority(bucketName), path.plain)
  def toAzure(bucketName: Hostname): Url.Plain = copy(scheme = "https", authority = Authority(bucketName), path.plain)
  def toSftp(authority: Authority): Url.Plain  = copy(scheme = "sftp", authority = authority, path.plain)

  def /[AA](path: Path[AA]): Url.Plain = copy(path = this.path./(path.show))
  def /(segment: String): Url.Plain    = copy(path = path./(segment))
  def /(segment: Option[String]): Url.Plain = segment match {
    case Some(s) => /(s)
    case None    => copy(path = path.plain)
  }

  /** Ensure that path always is suffixed with '/'
    */
  def `//`(segment: String): Url.Plain = copy(path = path.`//`(segment))
  def `//`(segment: Option[String]): Url.Plain = segment match {
    case Some(s) => `//`(s)
    case None    => copy(path = path.plain)
  }

  /** Safe toString implementation.
    *
    * @return Outputs user segment if any, will not print passwords
    */
  override val toString: String = {
    val sep = "://"

    authority match {
      case Authority(h, Some(u), Some(p)) =>
        show"${scheme.stripSuffix(sep)}$sep${u.user}@$h:$p/${path.show.stripPrefix("/")}"
      case Authority(h, Some(u), None) => show"${scheme.stripSuffix(sep)}$sep${u.user}@$h/${path.show.stripPrefix("/")}"
      case Authority(h, None, Some(p)) => show"${scheme.stripSuffix(sep)}$sep$h:$p/${path.show.stripPrefix("/")}"
      case Authority(h, None, None)    => show"${scheme.stripSuffix(sep)}$sep$h/${path.show.stripPrefix("/")}"
    }
  }

  /** Safe toString implementation
    *
    * @return Outputs masked passwords
    */
  val toStringMasked: String = {
    val sep = "://"

    authority match {
      case Authority(h, Some(u), Some(p)) => show"${scheme.stripSuffix(sep)}$sep$u@$h:$p/${path.show.stripPrefix("/")}"
      case Authority(h, Some(u), None)    => show"${scheme.stripSuffix(sep)}$sep$u@$h/${path.show.stripPrefix("/")}"
      case Authority(h, None, Some(p))    => show"${scheme.stripSuffix(sep)}$sep$h:$p/${path.show.stripPrefix("/")}"
      case Authority(h, None, None)       => show"${scheme.stripSuffix(sep)}$sep$h/${path.show.stripPrefix("/")}"
    }
  }

  def toStringWithPassword: String = {
    val sep = "://"

    authority match {
      case Authority(h, Some(u), Some(p)) =>
        show"${scheme.stripSuffix(sep)}$sep${u.toStringWithPassword}@$h:$p/${path.show.stripPrefix("/")}"
      case Authority(h, Some(u), None) =>
        show"${scheme.stripSuffix(sep)}$sep${u.toStringWithPassword}@$h/${path.show.stripPrefix("/")}"
      case Authority(h, None, Some(p)) => show"${scheme.stripSuffix(sep)}$sep$h:$p/${path.show.stripPrefix("/")}"
      case Authority(h, None, None)    => show"${scheme.stripSuffix(sep)}$sep$h/${path.show.stripPrefix("/")}"
    }
  }

}

object Url {

  type Plain = Url[String]

  def parseF[F[_]: ApplicativeError[*[_], Throwable]](c: String): F[Url.Plain] =
    parse(c).leftMap(MultipleUrlValidationException.apply).liftTo[F]

  def unsafe(c: String): Url.Plain = parse(c) match {
    case Valid(u)   => u
    case Invalid(e) => throw MultipleUrlValidationException(e) // scalafix:ok
  }

  def parse(c: String): ValidatedNec[UrlParseError, Url.Plain] = {
    val regex = "^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?".r

    // Treat `m.group` as unsafe, since it really is
    def tryOpt[A](a: => A): Try[Option[A]] = Try(a).map(Option.apply)

    def parseFileUrl(u: String): ValidatedNec[UrlParseError, Url.Plain] = {
      val fileRegex = "file:/([^:]+)".r
      fileRegex.findFirstMatchIn(u).map { m =>
        val matchRegex = tryOpt(m.group(1)).toEither.leftMap(_ => InvalidFileUrl(show"Not a valid file uri: $u"))
        OptionT(matchRegex.leftWiden[UrlParseError])
          .getOrElseF(InvalidFileUrl(show"File uri didn't match regex: ${fileRegex.pattern.toString}").asLeft[String])
          .map { pathPart =>
            if (!pathPart.startsWith("/"))
              Url("file", Authority.localhost, Path("/" + pathPart))
            else Url("file", Authority.localhost, Path(pathPart.stripPrefix("/")))
          }
          .toValidatedNec
      }.getOrElse(InvalidFileUrl(show"File uri didn't match regex: ${fileRegex.pattern.toString}").invalidNec)
    }

    lazy val parseNonFile = regex.findFirstMatchIn(c).map { m =>
      val authority: Either[AuthorityParseError, String] = OptionT(
        tryOpt(m.group(4)).toEither.leftMap(InvalidHost).leftWiden[AuthorityParseError]
      ).getOrElseF(MissingHost(c).asLeft)

      val typedAuthority: ValidatedNec[AuthorityParseError, Authority] =
        authority.leftMap(NonEmptyChain(_)).flatMap(Authority.parse(_).toEither).toValidated

      // Default to rootless paths when parsing URLs, these tends to be more useful.
      // S3 fails if you pass it an absolute path, the same will GCS. Rootless paths will work for SFTP servers that
      // don't use chroot for user logins, while absolute paths will not.
      val pathGroup        = OptionT(tryOpt(m.group(5))).map(_.stripPrefix("/"))
      val path: Path.Plain = pathGroup.map(Path.apply).getOrElse(Path.empty).getOrElse(Path.empty)
      val scheme =
        OptionT(
          tryOpt(m.group(2)).toEither.leftMap(t => MissingScheme(c, Some(t))).leftWiden[UrlParseError]
        ).getOrElseF(MissingScheme(c, None).asLeft[String]).toValidatedNec

      (scheme, typedAuthority).mapN((s, a) => Url(s, a, path))
    }.getOrElse(CouldntParseUrl(c).invalidNec)

    if (c.startsWith("file")) parseFileUrl(c) else parseNonFile
  }

  implicit def ordering[A]: Ordering[Url[A]] = _.show compare _.show
  implicit def order[A]: Order[Url[A]]       = Order.fromOrdering
  implicit def show[A]: Show[Url[A]] = u => {
    val pathString = u.path match {
      case a @ AbsolutePath(_, _) => a.show.stripPrefix("/")
      case a                      => a.show
    }

    if (u.scheme === "file") show"${u.scheme}:///$pathString" else show"${u.scheme}://${u.authority}/$pathString"
  }

}
