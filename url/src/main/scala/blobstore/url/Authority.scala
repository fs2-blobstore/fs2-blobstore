package blobstore.url

import blobstore.url.exception.{AuthorityParseError, MultipleUrlValidationException}
import cats.{ApplicativeThrow, Order, Show}
import cats.data.{NonEmptyChain, ValidatedNec}
import cats.data.Validated.{Invalid, Valid}
import cats.kernel.Eq
import cats.syntax.all.*

/** An authority as defined by RFC3986. Can point to any valid host on a computer network. Characterized by supporting
  * userinfo, port as well as IP addresses in addition to normal hostnames.
  *
  * @param host
  *   A valid host. This is either a domain name, or an IPv4 or IPv6 address
  * @param userInfo
  *   Optional userinfo component holding username and optionally password
  * @param port
  *   Optional port component
  * @see
  *   https://www.ietf.org/rfc/rfc3986.txt chapter 3.2 Authority
  */
case class Authority(host: Host, userInfo: Option[UserInfo] = None, port: Option[Port] = None) {

  override def toString: String = super.toString

  def toStringWithPassword: String = {
    val p = port.fold("")(p => show":$p")
    val u = userInfo.fold("")(u => show"${u.toStringWithPassword}@")
    show"$u$host$p"
  }
}

object Authority {

  /** input string bob:vacuum2000@example.com:8080
    *
    * results in the following subexpression matches
    *
    * $1 = bob $2 = vacuum2000 $3 = example.com $4 = 8080
    *
    * Group 3 is mandatory, all other are optional
    */
  private val regex = "^(?:([^:@]+)(?::([^@]+))?@)?([^:/@]+)(?::([0-9]+))?$".r

  def unsafe(candidate: String): Authority = parse(candidate) match {
    case Valid(a)   => a
    case Invalid(e) => throw MultipleUrlValidationException(e) // scalafix:ok
  }

  def localhost: Authority = unsafe("localhost")

  def parseF[F[_]: ApplicativeThrow](host: String): F[Authority] =
    parse(host).toEither.leftMap(MultipleUrlValidationException.apply).liftTo[F]

  def parse(candidate: String): ValidatedNec[AuthorityParseError, Authority] =
    regex.findFirstMatchIn(candidate).toRight(
      NonEmptyChain(AuthorityParseError.InvalidFormat(candidate, regex.pattern.pattern()))
    ).flatMap { m =>
      val password = Option(m.group(2))
      val user     = Option(m.group(1)).map(UserInfo(_, password))

      val host = Option(m.group(3)).map { h =>
        Hostname.parse(h)
      }.getOrElse(AuthorityParseError.MissingHostname(candidate).invalidNec)

      val port = Option(m.group(4)).traverse(p => Port.parse(p).toValidatedNec)

      (host, user.validNec, port).mapN(Authority.apply).toEither
    }.toValidated

  implicit val show: Show[Authority]         = s => s.host.show + s.port.map(_.show).map(":" + _).getOrElse("")
  implicit val order: Order[Authority]       = Order.by(_.show)
  implicit val ordering: Ordering[Authority] = order.toOrdering
  implicit val eq: Eq[Authority] = (x, y) => {
    x.host === y.host && x.port === y.port
  }
}
