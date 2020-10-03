package blobstore.url

import blobstore.url.Authority.Bucket
import cats.data.Validated.{Invalid, Valid}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import cats.syntax.all._
import cats.instances.list._
import org.scalatest.Inside

class AuthorityTest extends AnyFlatSpec with Matchers with Inside {

  it should "parse bucket names" in {
    val valid = List(
      "foo",
      "foo_bar.baz-bam42"
    ).traverse(Bucket.parse)

    val invalid = List(
      "",
      "foo@bar",
      "foo..bar",
      "foo.bar.",
      "192.168.0.1",
      (1 until 64).toList.as("a").mkString,
      "a",
      "aa"
    ).traverse(Bucket.parse)

    inside(valid) {
      case Valid(_) => // ok
    }

    inside(invalid) {
      case Invalid(_) => // ok
    }
  }
}
