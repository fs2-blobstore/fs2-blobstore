package blobstore.url

import cats.syntax.all._
import weaver.FunSuite

import scala.util.Try

object HostnameTest extends FunSuite {

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

  test("allow valid hostnames") {
    validHostnames.map { h =>
      expect.all(
        Hostname.parse(h).isValid,
        Hostname.parseF[Try](h).isSuccess
      )
    }.combineAll
  }

  test("not allow invalid hostnames") {
    invalidHostnames.map { h =>
      expect.all(
        Hostname.parse(h).isInvalid,
        Hostname.parseF[Try](h).isFailure
      )
    }.combineAll
  }
}
