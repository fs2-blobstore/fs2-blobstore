package blobstore.url

import blobstore.url.exception.{PortParseError, SingleValidationException}
import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import cats.instances.either._
import cats.instances.int._
import cats.kernel.Order
import cats.syntax.all._
import cats.{ApplicativeError, Show}

import scala.util.Try

case class Port(portNumber: Int) {
  override def toString: String = portNumber.toString
}

object Port {
  val MinPortNumber: Int = 0
  val MaxPortNumber: Int = 65535

  def parse(c: String): Validated[PortParseError, Port] =
    Try(c.toInt).toEither.leftMap(_ => PortParseError.InvalidPort(c)).leftWiden[PortParseError].flatMap { i =>
      if (i < MinPortNumber || i > MaxPortNumber) PortParseError.PortNumberOutOfRange(i).asLeft
      else new Port(i).asRight
    }.toValidated

  def parseF[F[_]: ApplicativeError[*[_], Throwable]](c: String): F[Port] =
    parse(c).toEither.leftMap(SingleValidationException(_)).liftTo[F]

  def unsafe(c: String): Port = parse(c) match {
    case Valid(p)   => p
    case Invalid(e) => throw SingleValidationException(e) // scalafix:ok
  }

  def compare(one: Port, two: Port): Int = one.portNumber compare two.portNumber

  implicit val ordering: Ordering[Port] = compare
  implicit val order: Order[Port]       = Order.fromOrdering
  implicit val show: Show[Port]         = _.portNumber.show
}
