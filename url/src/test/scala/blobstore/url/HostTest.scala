package blobstore.url

import cats.syntax.all._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.Inside

import scala.util.{Failure, Success, Try}

class HostTest extends AnyFlatSpec with Matchers with Inside {
  behavior of "Host"

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

  it should "parse" in {
    validHostnames.map(Host.parse).map(_.toEither).foreach { h =>
      inside(h) {
        case Right(Hostname(_)) => //noop
      }
    }
    validHostnames.map(Host.parseF[Try]).foreach { h =>
      inside(h) {
        case Success(Hostname(_)) => //noop
      }
    }

    validIps.map(Host.parse).map(_.toEither).foreach { h =>
      inside(h) {
        case Right(IpV4Address(_, _, _, _)) => //noop
      }
    }

    validIps.map(Host.parseF[Try]).foreach { h =>
      inside(h) {
        case Success(IpV4Address(_, _, _, _)) => //noop
      }
    }

    invalidHostnames.map(Host.parse).map(_.toEither).foreach { h =>
      inside(h) {
        case Left(_) => //noop
      }
    }

    invalidHostnames.map(Hostname.parseF[Try]).foreach { h =>
      inside(h) {
        case Failure(_) => //noop
      }
    }

    invalidIps.map(IpV4Address.parse).map(_.toEither).foreach { h =>
      inside(h) {
        case Left(_) => //noop
      }
    }

    invalidIps.map(IpV4Address.parseF[Try]).foreach { h =>
      inside(h) {
        case Failure(_) => //noop
      }
    }
  }

  it should "order" in {
    val sorted = (validHostnames ++ validIps).map(Host.parse(_).toOption).flattenOption.map(_.show).sorted
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

    sorted must contain theSameElementsInOrderAs correctSorting
  }
}
