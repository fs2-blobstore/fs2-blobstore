package blobstore.url

import cats.data.Validated
import cats.syntax.all._
import weaver.FunSuite

import scala.util.{Success, Try}

object HostTest extends FunSuite {

  val validHostnames: List[String] = List(
    "f",
    "foo",
    "foo.com",
    "7foo.com",
    (1 to 63).toList.as("t").mkString("")
  )

  val invalidHostnames: List[String] = List(
    "",
    "foo..bar.com",
    ".foo.bar.com",
    "*foo.bar.com",
    (1 to 64).toList.as("t").mkString("")
  )

  val validIps: List[String] = List(
    "1.1.1.1",
    "255.255.255.255",
    "123.223.123.8"
  )

  val invalidIps: List[String] = List(
    "",
    "foo",
    "1.1.1.256",
    "1.1.1"
  )

  test("parse") {

    val hostnames = validHostnames.map(s => Host.parse(s) -> Host.parseF[Try](s)).map { case (parsed, parsedTry) =>
      expect.all(
        parsed match {
          case Validated.Valid(Hostname(_)) => true
          case _                            => false
        },
        parsedTry match {
          case Success(Hostname(_)) => true
          case _                    => false
        }
      )
    }.combineAll

    val ips = validIps.map(s => Host.parse(s) -> Host.parseF[Try](s)).map { case (parsed, parsedTry) =>
      expect.all(
        parsed match {
          case Validated.Valid(IpV4Address(_, _, _, _)) => true
          case _                                        => false
        },
        parsedTry match {
          case Success(IpV4Address(_, _, _, _)) => true
          case _                                => false
        }
      )
    }.combineAll

    val nonHostnames =
      invalidHostnames.map(s => Host.parse(s) -> Host.parseF[Try](s)).map { case (parsed, parsedTry) =>
        expect.all(parsed.isInvalid, parsedTry.isFailure)
      }.combineAll

    val nonIps =
      invalidIps.map(s => IpV4Address.parse(s) -> IpV4Address.parseF[Try](s)).map { case (parsed, parsedTry) =>
        expect.all(parsed.isInvalid, parsedTry.isFailure)
      }.combineAll

    hostnames and ips and nonHostnames and nonIps
  }

  test("order") {
    val sorted = (validHostnames ++ validIps).flatMap(Host.parse(_).toOption).sorted(Host.order.toOrdering).map(_.show)
    val correctSorting =
      List(
        "1.1.1.1",
        "123.223.123.8",
        "255.255.255.255",
        "7foo.com",
        "f",
        "foo",
        "foo.com",
        "ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt"
      )

    expect(sorted == correctSorting)
  }
}
