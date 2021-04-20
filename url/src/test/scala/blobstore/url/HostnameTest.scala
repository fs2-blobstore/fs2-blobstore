package blobstore.url

import cats.data.Validated.{Invalid, Valid}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.Inside

import scala.util.{Failure, Success, Try}

class HostnameTest extends AnyFlatSpec with Matchers with Inside {

  behavior of "Hostname"

  private val validHostnames = List(
    "foo",
    "foo-bar",
    "FOOBAR",
    "foo_bar",
    "foo_bar123",
    "a".repeat(63),
    "a",
    "A",
    "foo.bar-baz"
  )

  val invalidHostnames = List(
    "",
    "f".repeat(64),
    "foob#r".repeat(64)
  )

  it should "allow valid hostnames" in {
    validHostnames.foreach { h =>
      inside(Hostname.parse(h)) {
        case Valid(_) => // noop
      }

      inside(Hostname.parseF[Try](h)) {
        case Success(_) => // noop
      }
    }
  }

  it should "not allow invalid hostnames" in {
    invalidHostnames.foreach { h =>
      inside(Hostname.parse(h)) {
        case Invalid(_) => // noop
      }

      inside(Hostname.parseF[Try](h)) {
        case Failure(_) => // noop
      }
    }
  }
}
