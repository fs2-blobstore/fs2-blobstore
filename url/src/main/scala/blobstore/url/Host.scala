package blobstore.url

import blobstore.url.exception.{HostParseError, MultipleUrlValidationException}
import blobstore.url.Hostname.Label
import blobstore.url.exception.HostParseError.label
import cats.{ContravariantMonoidal, Order, Show}
import cats.data.{NonEmptyChain, Validated, ValidatedNec}
import cats.data.Validated.{Invalid, Valid}
import cats.instances.either._
import cats.instances.int._
import cats.instances.list._
import cats.instances.order._
import cats.instances.string._
import cats.syntax.all._
import shapeless.{Const, Sized}
import shapeless.nat._
import shapeless.HList._
import shapeless.PolyDefns.~>

import scala.util.Try
import scala.util.matching.Regex

/**
  * A Host is any device or computer on a computer network. It can be an IP address or any string that can appear in a
  * A, AAAA or CNAME DNS record.
  *
  * @see https://www.ietf.org/rfc/rfc1123.txt 2.1 "Host names and numbers"
  */
sealed trait Host

object Host {

  def parse(s: String): ValidatedNec[HostParseError, Host] = Hostname.parse(s).orElse(IpV4Address.parse(s))

  def unsafe(s: String): Host = parse(s) match {
    case Valid(v)   => v
    case Invalid(e) => throw MultipleUrlValidationException(e)
  }

  implicit def order: Order[Host] =
    ContravariantMonoidal[Order].liftContravariant[Host, Either[IpV4Address, Hostname]] {
      case a @ IpV4Address(_, _, _, _) => Left(a)
      case b @ Hostname(_)             => Right(b)
    }(Order[Either[IpV4Address, Hostname]])

  implicit val ordering: Ordering[Host] = order.toOrdering

  implicit val show: Show[Host] = {
    case a @ IpV4Address(_, _, _, _) => a.show
    case a @ Hostname(_)             => a.show
  }

}

case class IpV4Address(octet1: Byte, octet2: Byte, octet3: Byte, octet4: Byte) extends Host {
  val bytes: Sized[IndexedSeq[Byte], _4] = Sized(octet1, octet2, octet3, octet4)

  override def toString: String = s"${octet1 & 0xff}.${octet2 & 0xff}.${octet3 & 0xff}.${octet4 & 0xff}"
}

object IpV4Address {
  def parse(s: String): ValidatedNec[HostParseError, IpV4Address] = {
    def predicate(octet: String): ValidatedNec[HostParseError, Byte] =
      Try(octet.toInt).toEither.leftMap(_ => HostParseError.ipv4.InvalidIpv4(octet)).flatMap { o =>
        if (o >= 0 && o <= 255) o.toByte.asRight else HostParseError.ipv4.OctetOutOfRange(o, octet).asLeft
      }.toValidatedNec

    s.split('.').toList match {
      case one :: two :: three :: four :: Nil =>
        (predicate(one), predicate(two), predicate(three), predicate(four)).mapN(IpV4Address.apply)
      case _ => HostParseError.ipv4.InvalidIpv4(s).invalidNec
    }
  }

  def unsafe(s: String): IpV4Address = parse(s) match {
    case Valid(i)   => i
    case Invalid(e) => throw MultipleUrlValidationException(e)
  }

  private object compareBytes extends (Const[(Byte, Byte)]#λ ~> Const[Int]#λ) {
    override def apply[T](f: (Byte, Byte)): Int = (f._1 & 0xff) compare (f._2 & 0xff)
  }

  def compare(x: IpV4Address, y: IpV4Address): Int =
    x.bytes.toHList.zip(y.bytes.toHList).foldMap(0)(IpV4Address.compareBytes)(_ + _)

  implicit val ordering: Ordering[IpV4Address] = compare
  implicit val order: Order[IpV4Address]       = Order.fromOrdering[IpV4Address]
  implicit val show: Show[IpV4Address]         = Show.fromToString[IpV4Address]
}

case class Hostname private (labels: NonEmptyChain[Label]) extends Host {
  val value: String             = labels.mkString_(".")
  override def toString: String = value
}

object Hostname {

  /**
    * Label component of a hostname.
    *
    * Rules
    * - 1 to 63 characters
    * - Only consists of characters a-z, A-Z, 0-9 or a hyphen
    *
    * RFC1123, section 2.1 "Host Names and Numbers"
    *
    * @see http://www.ietf.org/rfc/rfc1123.txt
    */
  case class Label private[Hostname] (value: String) {
    override def toString: String = value
  }

  object Label {
    val Regex: Regex = "[-_a-zA-Z0-9]+".r

    implicit val show: Show[Label]         = _.value
    implicit val order: Order[Label]       = _.value compare _.value
    implicit val ordering: Ordering[Label] = order.toOrdering
  }

  def parse(value: String): ValidatedNec[HostParseError, Hostname] = {
    val labels: ValidatedNec[HostParseError, List[Label]] = value.split('.').toList.traverse { el =>
      val lengthOk: ValidatedNec[HostParseError, Unit] = Validated.cond(
        el.length >= 3 && el.length <= 63,
        (),
        HostParseError.label.LabelLengthOutOfRange(el)
      ).toValidatedNec
      val charactersOk: ValidatedNec[HostParseError, Unit] =
        Validated.cond(Label.Regex.matches(el), (), HostParseError.label.InvalidCharactersInLabel(el)).toValidatedNec

      (lengthOk, charactersOk).tupled.as(Label(el))
    }

    val labelsNec: ValidatedNec[HostParseError, NonEmptyChain[Label]] = labels.toEither.flatMap {
      case h :: t => NonEmptyChain(h, t: _*).asRight[NonEmptyChain[HostParseError]]
      case Nil    => NonEmptyChain(HostParseError.hostname.EmptyDomainName).asLeft.leftWiden[NonEmptyChain[HostParseError]]
    }.toValidated

    val length: ValidatedNec[HostParseError, Unit] =
      Validated.cond(
        value.length >= 1 && value.length <= 255,
        (),
        NonEmptyChain(HostParseError.hostname.DomainNameOutOfRange(value))
      )

    val firstCharacter: ValidatedNec[HostParseError, Unit] = value.headOption.map { h =>
      Validated.cond(h.isLetterOrDigit, (), HostParseError.hostname.InvalidFirstCharacter(value)).toValidatedNec
    }.getOrElse(HostParseError.hostname.EmptyDomainName.invalidNec[Unit])

    (labelsNec, length, firstCharacter).mapN((labels, _, _) => Hostname(labels))
  }

  def unsafe(value: String): Hostname = parse(value) match {
    case Valid(h)   => h
    case Invalid(e) => throw MultipleUrlValidationException(e)
  }

  def compare(a: Hostname, b: Hostname): Int = a.show compare b.show

  implicit val ordering: Ordering[Hostname] = compare
  implicit val order: Order[Hostname]       = Order.fromOrdering[Hostname]
  implicit val show: Show[Hostname]         = _.labels.toList.mkString(".")
}
