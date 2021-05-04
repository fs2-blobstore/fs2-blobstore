package blobstore.url

import cats.syntax.all._
import weaver.FunSuite

object AuthorityTest extends FunSuite {

  test("parse bucket names") {
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

    expect(valid.isValid)
    expect(invalid.isInvalid)
  }
}
