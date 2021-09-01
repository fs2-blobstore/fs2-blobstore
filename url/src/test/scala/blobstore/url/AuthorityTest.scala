package blobstore.url

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import cats.syntax.all.*
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

    valid.isValid mustBe true

    invalid.isInvalid mustBe true
  }
}
