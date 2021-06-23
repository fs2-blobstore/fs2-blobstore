package blobstore.url.exception

import cats.syntax.all.*
import cats.Show
import cats.kernel.Semigroup

sealed trait UrlParseError {
  def error: String
  def cause: Option[Throwable] = None
}

object UrlParseError {
  case class CouldntParseUrl(i: String) extends SchemeError { val error = show"Value is not a valid URL $i" }

  case class MissingScheme(i: String, override val cause: Option[Throwable]) extends SchemeError {
    val error = show"Couldn't identify scheme in $i"
  }
  implicit val semigroup: Semigroup[UrlParseError] =
    (x, y) =>
      new UrlParseError {
        override def error: String = show"${x.error}\n${y.error}"
        override def cause: Option[Throwable] =
          (x.cause, y.cause).mapN(Throwables.collapsingSemigroup.combine).orElse(x.cause).orElse(y.cause)
      }
}

sealed trait SchemeError extends UrlParseError

object SchemeError {

  case class IncorrectScheme(scheme: String, expected: String) extends SchemeError {
    val error = show"Expected scheme $expected, but got $scheme"
  }

  case class InvalidScheme(scheme: String) extends SchemeError { val error = show"Invalid scheme, got $scheme" }

}

sealed trait AuthorityParseError extends UrlParseError

object AuthorityParseError {
  case class InvalidHostname(c: String) extends AuthorityParseError { val error = show"Invalid hostname $c" }
  case class MissingHostname(c: String) extends AuthorityParseError { val error = show"Missing hostname $c" }
  case class InvalidHost(t: Throwable) extends AuthorityParseError {
    val error                             = show"Invalid host, got: ${t.getMessage}"
    override val cause: Option[Throwable] = Some(t)
  }
  case class MissingHost(i: String) extends AuthorityParseError { val error = show"Missing host: $i" }
  case class InvalidFormat(c: String, format: String) extends AuthorityParseError {
    val error = show"Invalid format. Authorities must be on format $format, got $c"
  }
  case class HostWasEmpty(c: String) extends AuthorityParseError { val error = show"Host was empty: $c" }

  case class InvalidFileUrl(m: String) extends AuthorityParseError { val error = m }

  implicit val show: Show[AuthorityParseError] = _.error
}

sealed trait PortParseError extends AuthorityParseError
object PortParseError {
  case class InvalidPort(s: String) extends PortParseError { val error = show"Invalid port numbers $s" }
  case class PortNumberOutOfRange(i: Int) extends PortParseError {
    val error = show"Port number out of range [0,65535]: $i"
  }

}

sealed trait HostParseError extends AuthorityParseError

object HostParseError {
  object label {
    case class LabelLengthOutOfRange(label: String) extends HostParseError {
      val error = show"Label length out of range (min 1, max 63): $label"
    }
    case class InvalidCharactersInLabel(label: String) extends HostParseError {
      val error = show"Hostname labels may only contain characters in a-z, A-Z, 0-9 and hyphen, $label"
    }

  }
  object hostname {
    case object EmptyDomainName extends HostParseError { val error = show"Domain name may not be empty" }
    case class DomainNameOutOfRange(domainName: String) extends HostParseError {
      val error = show"Domain name out of range (min: 1, max: 255): $domainName"
    }
    case class InvalidFirstCharacter(domainName: String) extends HostParseError {
      val error = show"First character in a domain host name must be a letter or digit: $domainName"
    }
  }

  object ipv4 {
    case class OctetOutOfRange(octet: Int, input: String) extends HostParseError {
      val error = show"Octet out of range for ipv4, $octet, input: $input"
    }
    case class InvalidIpv4(input: String) extends HostParseError { val error = show"Not a valid ipv4 address $input" }
  }
}
