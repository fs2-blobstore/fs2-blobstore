package blobstore.url

import blobstore.url.exception.{AuthorityParseError, BucketParseError, MultipleUrlValidationException}
import cats.{ApplicativeError, ContravariantMonoidal, Order, Show}
import cats.data.{NonEmptyChain, ValidatedNec}
import cats.data.Validated.{Invalid, Valid}
import cats.instances.either._
import cats.instances.option._
import cats.instances.order._
import cats.instances.string._
import cats.syntax.all._

sealed trait Authority {
  def host: Host
}

object Authority {

  /**
    * An authority as defined by RFC3986. Can point to any valid host on a computer network. Characterized by supporting
    * userinfo, port as well as IP addresses in addition to normal hostnames.
    *
    * @param host A valid host. This is either a domain name, or an IPv4 or IPv6 address
    * @param userInfo Optional userinfo component holding username and optionally password
    * @param port Optional port component
    * @see https://www.ietf.org/rfc/rfc3986.txt chapter 3.2 Authority
    */
  case class Standard(host: Host, userInfo: Option[UserInfo], port: Option[Port]) extends Authority {
    lazy val toBucket: ValidatedNec[BucketParseError, Bucket] = Bucket.parse(Show[Standard].show(this))

    def equalsIgnoreUserInfo(a: Authority): Boolean = a match {
      case Standard(host, _, port) => this.host === host && this.port === port
      case Bucket(name, _) => this.host === name && port.isEmpty
    }
  }

  object Standard {

    /**
      * input string bob:vacuum2000@example.com:8080
      *
      * results in the following subexpression matches
      *
      * $1 = bob
      * $2 = vacuum2000
      * $3 = example.com
      * $4 = 8080
      *
      * Group 3 is mandatory, all other are optional
      */
    private val regex = "^(?:([^:@]+)(?::([^@]+))?@)?([^:/@]+)(?::([0-9]+))?$".r

    def apply(candidate: String): ValidatedNec[AuthorityParseError, Standard] = parse(candidate)
    def unsafe(candidate: String): Standard = parse(candidate) match {
      case Valid(a) => a
      case Invalid(e) => throw MultipleUrlValidationException(e)
    }

    def localhost: Standard = unsafe("localhost")

    def parseF[F[_]: ApplicativeError[*[_], Throwable]](host: String): F[Standard] =
      parse(host).toEither.leftMap(MultipleUrlValidationException).liftTo[F]

    def parse(candidate: String): ValidatedNec[AuthorityParseError, Standard] =
      regex.findFirstMatchIn(candidate).toRight(
        NonEmptyChain(AuthorityParseError.InvalidFormat(candidate, regex.pattern.pattern()))
      ).flatMap { m =>
        val password = Option(m.group(2))
        val user     = Option(m.group(1)).map(UserInfo(_, password))

        val host = Option(m.group(3)).map { h =>
          Hostname.parse(h)
        }.getOrElse(AuthorityParseError.MissingHostname(candidate).invalidNec)

        val port = Option(m.group(4)).traverse(p => Port.parse(p).toValidatedNec)

        (host, user.validNec, port).mapN(Standard.apply).toEither
      }.toValidated

    implicit val show: Show[Standard]   = s => s.host.show + s.port.map(_.show).map(":" + _).getOrElse("")
    implicit val order: Order[Standard] = Order.by(_.show)
    implicit val ordering: Ordering[Standard] = order.toOrdering
  }

  /**
    * A bucket is a special class of authorities typically associated with cloud storage providers. Buckets are only
    * identifiable by a domain name, and they never have userinfo or ports associated with them. Optionally buckets may
    * have a vendor specific region.
    *
    * @param name The domain name identifying the bucket
    * @param region An optional region associated with the bucket
    */
  case class Bucket(name: Hostname, region: Option[String]) extends Authority {
    override val host: Host = name

    def withRegion(region: String): Bucket = new Bucket(name, Some(region))

    def toStandardAuthority: Standard = Standard(name, None, None)
  }

  object Bucket {

    def apply(c: String): ValidatedNec[BucketParseError, Bucket] = parse(c)

    def withAuthority(s: Authority): Option[Bucket] = s match {
      case Standard(h@Hostname(_), None, None) => Some(new Bucket(h, None))
      case b: Bucket => Some(b)
      case _ => None
    }

    def parse(c: String): ValidatedNec[BucketParseError, Bucket] =
      Standard.parse(c).leftMap(nec => NonEmptyChain(BucketParseError.InvalidAuthority(nec))).toEither.flatMap { a =>
        val validatePort = a.port.fold(().asRight[BucketParseError])(p => BucketParseError.PortNotSupported(p).asLeft).toValidatedNec
        val validateUserinfo = a.userInfo.fold(().asRight[BucketParseError])(u => BucketParseError.UserInfoNotSupported(u).asLeft).toValidatedNec

        val hostname = a.host match {
          case h: Hostname => h.asRight
          case a => BucketParseError.HostnameRequired(a).asLeft
        }

        (validatePort, validateUserinfo, hostname.toValidatedNec).mapN((_, _, h) => h).map(new Bucket(_, None)).toEither
      }.toValidated

    def parseF[F[_]: ApplicativeError[*[_], Throwable]](c: String): F[Bucket] =
      parse(c).toEither.leftMap(MultipleUrlValidationException).liftTo[F]

    def unsafe(c: String): Bucket =
      parse(c) match {
        case Valid(a) => a
        case Invalid(e) => throw MultipleUrlValidationException(e)
      }

    def parseWithRegion(c: String, region: String): ValidatedNec[BucketParseError, Bucket] =
      parse(c).map(_.withRegion(region))

    def parseWithRegionF[F[_]: ApplicativeError[*[_], Throwable]](c: String, region: String): F[Bucket] =
      parseF[F](c).map(_.withRegion(region))

    def unsafeWithRegion(c: String, region: String): Bucket = unsafe(c).withRegion(region)

    implicit val order: Order[Bucket] = _.name compare _.name
    implicit val ordering: Ordering[Bucket] = order.toOrdering
    implicit val show: Show[Bucket] = _.name.show
  }

  def parse(s: String): ValidatedNec[AuthorityParseError, Authority] = Standard.parse(s)

  type AuthorityCoproduct = Either[Standard, Bucket]

  private val authorityToCoproduct: Authority => AuthorityCoproduct = {
    case u: Standard => Left(u)
    case b: Bucket => Right(b)
  }

  implicit val order: Order[Authority] =
    ContravariantMonoidal[Order].liftContravariant[Authority, AuthorityCoproduct](authorityToCoproduct)(Order[AuthorityCoproduct])

  implicit val ordering: Ordering[Authority] = order.toOrdering

  implicit val show: Show[Authority] = {
    case url: Standard => url.show
    case b: Bucket => b.show
  }

}
