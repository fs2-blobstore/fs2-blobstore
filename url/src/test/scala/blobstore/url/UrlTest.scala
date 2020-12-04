package blobstore.url

import blobstore.url.Authority.Standard
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.Inside
import cats.syntax.all._

class UrlTest extends AnyFlatSpec with Matchers with Inside {

  behavior of "Url"

  it should "compose" in {
    val fileLike = "https://foo.example.com"

    inside(Url.bucket(fileLike).toEither) {
      case Right(url) =>
        (url / "foo").show mustBe "https://foo.example.com/foo"
        (url / "/foo").show mustBe "https://foo.example.com/foo"
        (url / "//foo").show mustBe "https://foo.example.com//foo"
        (url / Some("foo")).show mustBe "https://foo.example.com/foo"
        (url / Path("foo")).show mustBe "https://foo.example.com/foo"
        (url / Path("/foo")).show mustBe "https://foo.example.com/foo"
        (url `//` "foo").show mustBe "https://foo.example.com/foo/"
        (url / "foo").replacePath(Path("bar/baz/")).show mustBe "https://foo.example.com/bar/baz/"
    }
  }

  it should "render correctly with toString" in {
    val url1 = Url(
      scheme = "https",
      authority = Standard(Host.unsafe("foo.com"), UserInfo("foo", Some("bar")).some, Some(Port.unsafe(8080))),
      path = Path("foo")
    )
    val url2 = Url(
      scheme = "https",
      authority = Standard(Host.unsafe("foo.com"), UserInfo("foo", None).some, Some(Port.unsafe(8080))),
      path = Path("foo")
    )
    val url3 = Url(
      scheme = "https",
      authority = Standard(Host.unsafe("foo.com"), None, Some(Port.unsafe(8080))),
      path = Path("foo")
    )
    val url4 = Url(
      scheme = "https",
      authority = Standard(Host.unsafe("foo.com"), None, None),
      path = Path("foo")
    )

    url1.toString mustBe "https://foo@foo.com:8080/foo"
    url1.toStringMasked mustBe "https://foo:*****@foo.com:8080/foo"
    url1.toStringWithPassword mustBe "https://foo:bar@foo.com:8080/foo"

    url2.toString mustBe "https://foo@foo.com:8080/foo"
    url2.toStringMasked mustBe "https://foo@foo.com:8080/foo"
    url2.toStringWithPassword mustBe "https://foo@foo.com:8080/foo"

    url3.toString mustBe "https://foo.com:8080/foo"
    url3.toStringMasked mustBe "https://foo.com:8080/foo"
    url3.toStringWithPassword mustBe "https://foo.com:8080/foo"

    url4.toString mustBe "https://foo.com/foo"
    url4.toStringMasked mustBe "https://foo.com/foo"
    url4.toStringWithPassword mustBe "https://foo.com/foo"
  }
}
