package blobstore.url

import cats.data.Validated.{Invalid, Valid}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import cats.syntax.all._
import org.scalatest.Inside

class AuthorityTest extends AnyFlatSpec with Matchers with Inside {

  it should "parse bucket names" in {
    val valid = List(
      "foo",
      "foo_bar.baz-bam42"
    ).traverse(Authority.parse)

    val invalid = List(
      "",
      "foo@bar",
      "foo..bar",
      "foo.bar.",
      "192.168.0.1",
      (1 until 64).toList.as("a").mkString,
      "a",
      "aa"
    ).traverse(Authority.parse)

    inside(valid) {
      case Valid(_) => // ok
    }

    inside(invalid) { // scalafix:ok
      case Invalid(_) => // ok
    }
  }
}
