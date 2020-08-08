package blobstore.experiment.url

import blobstore.experiment.exception.{AuthorityParseError, BucketParseError, MultipleUrlValidationException}
import blobstore.experiment.url.Authority.{Bucket, StandardAuthority}
import cats.{ApplicativeError, ContravariantMonoidal, Order, Show}
import cats.data.{NonEmptyChain, ValidatedNec}
import cats.data.Validated.{Invalid, Valid}
import cats.instances.either._
import cats.instances.option._
import cats.instances.order._
import cats.instances.string._
import cats.syntax.all._

sealed trait Authority {
  def toStandardAuthority: StandardAuthority = this match {
    case u: StandardAuthority => u
    case b: Bucket => StandardAuthority(b.name, None, None)
  }

  def toBucket: Option[Bucket] = this match {
    case StandardAuthority(h@Hostname(_), None, None) => Some(new Bucket(h, None))
    case b: Bucket => Some(b)
    case _ => None
  }
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
  case class StandardAuthority(host: Host, userInfo: Option[UserInfo], port: Option[Port]) extends Authority

  object StandardAuthority {

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

    def apply(candidate: String): ValidatedNec[AuthorityParseError, StandardAuthority] = parse(candidate)
    def unsafe(candidate: String): StandardAuthority = parse(candidate) match {
      case Valid(a) => a
      case Invalid(e) => throw MultipleUrlValidationException(e)
    }

    def parseF[F[_]: ApplicativeError[*[_], Throwable]](host: String): F[StandardAuthority] =
      parse(host).toEither.leftMap(MultipleUrlValidationException).liftTo[F]

    def parse(candidate: String): ValidatedNec[AuthorityParseError, StandardAuthority] =
      regex.findFirstMatchIn(candidate).toRight(
        NonEmptyChain(AuthorityParseError.InvalidFormat(candidate, regex.pattern.pattern()))
      ).flatMap { m =>
        val password = Option(m.group(2))
        val user     = Option(m.group(1)).map(UserInfo(_, password))

        val host = Option(m.group(3)).map { h =>
          Hostname.parse(h)
        }.getOrElse(AuthorityParseError.MissingHostname(candidate).invalidNec)

        val port = Option(m.group(4)).traverse(p => Port.parse(p).toValidatedNec)

        (host, user.validNec, port).mapN(StandardAuthority.apply).toEither
      }.toValidated

    implicit val show: Show[StandardAuthority]   = s => s.host.show + s.port.map(_.show).map(":" + _).getOrElse("")
    implicit val order: Order[StandardAuthority] = Order.by(_.show)
  }

  /**
    * A bucket is a special class of authorities typically associated with cloud storage providers. Buckets are only
    * identifiable by a domain name, and they never have userinfo or ports associated with them. Optionally buckets may
    * have a vendor specific region.
    *
    * @param name The domain name identifying the bucket
    * @param region An optional region associated with the bucket
    */
  final class Bucket private[url] (val name: Hostname, val region: Option[String]) extends Authority with Product2[Hostname, Option[String]] with Serializable with Ordered[Bucket] {
    def withRegion(region: String): Bucket = new Bucket(name, Some(region))

    override val _1: Hostname = name

    override val _2: Option[String] = region

    override def compare(that: Bucket): Int = Bucket.compare(this, that)

    override def canEqual(that: Any): Boolean = that.isInstanceOf[Bucket]
  }

  object Bucket {

    def apply(c: String): ValidatedNec[BucketParseError, Bucket] = parse(c)

    def unapply(s: String): Option[Bucket] = Hostname.parse(s).toOption.map(h => new Bucket(h, None))

    def unapply(s: Authority): Option[Bucket] = s match {
      case StandardAuthority(h@Hostname(_), None, None) => Some(new Bucket(h, None))
      case b: Bucket => Some(b)
      case _ => None
    }

    def parse(c: String): ValidatedNec[BucketParseError, Bucket] =
      StandardAuthority.parse(c).leftMap(nec => NonEmptyChain(BucketParseError.InvalidAuthority(nec))).toEither.flatMap { a =>
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

    // Assume that two buckets are equal if they have the same name, disregard region
    def compare(one: Bucket, two: Bucket): Int = one.name compare two.name

    implicit val ordering: Ordering[Bucket] = compare
    implicit val order: Order[Bucket] = Order.fromOrdering
    implicit val show: Show[Bucket] = _.name.show
  }

  type AuthorityCoproduct = Either[StandardAuthority, Bucket]

  private val authorityToCoproduct: Authority => AuthorityCoproduct = {
    case u: StandardAuthority => Left(u)
    case b: Bucket => Right(b)
  }

  implicit val order: Order[Authority] =
    ContravariantMonoidal[Order].liftContravariant[Authority, AuthorityCoproduct](authorityToCoproduct)(Order[AuthorityCoproduct])

  implicit val ordering: Ordering[Authority] = order.compare _

  implicit val show: Show[Authority] = {
    case url: StandardAuthority => url.show
    case b: Bucket => b.show
  }


}
