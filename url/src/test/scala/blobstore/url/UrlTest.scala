package blobstore.url

import blobstore.url.exception.UrlParseError
import blobstore.url.UrlTest.{Password, User}
import blobstore.url.exception.UrlParseError.MissingScheme
import blobstore.url.Path.RootlessPath
import cats.data.Validated.{Invalid, Valid}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.Inside
import cats.syntax.all._

import scala.util.{Success, Try}

class UrlTest extends AnyFlatSpec with Matchers with Inside {

  behavior of "Url"

  // scalafix:off

  it should "compose" in {
    val fileLike = "https://foo.example.com"

    inside(Url.parse(fileLike).toEither) {
      case Right(url) =>
        (url / "foo").show mustBe "https://foo.example.com/foo"
        (url / "/foo").show mustBe "https://foo.example.com/foo"
        (url / "//foo").show mustBe "https://foo.example.com//foo"
        (url / Some("foo")).show mustBe "https://foo.example.com/foo"
        (url / Path("foo")).show mustBe "https://foo.example.com/foo"
        (url / Path("/foo")).show mustBe "https://foo.example.com/foo"
        (url `//` "foo").show mustBe "https://foo.example.com/foo/"
        (url / "foo").withPath(Path("bar/baz/")).show mustBe "https://foo.example.com/bar/baz/"
        (url / "foo").withAuthority(Authority.unsafe("bar.example.com")).show mustBe "https://bar.example.com/foo"
    }
  }

  it should "render correctly with toString" in {
    val url1 = Url(
      scheme = "https",
      authority = Authority(Host.unsafe("foo.com"), UserInfo("foo", Some("bar")).some, Some(Port.unsafe(8080))),
      path = Path("foo")
    )
    val url2 = Url(
      scheme = "https",
      authority = Authority(Host.unsafe("foo.com"), UserInfo("foo", None).some, Some(Port.unsafe(8080))),
      path = Path("foo")
    )
    val url3 = Url(
      scheme = "https",
      authority = Authority(Host.unsafe("foo.com"), None, Some(Port.unsafe(8080))),
      path = Path("foo")
    )
    val url4 = Url(
      scheme = "https",
      authority = Authority(Host.unsafe("foo.com"), None, None),
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

  behavior of "parsing"

  it should "parse valid urls" in {
    val schemes = List("gs", "s3", "sftp", "http", "https")
    val validUrls = List(
      "foo",
      "foo/",
      "foo/bar",
      "foo/bar/",
      "foo/bar/baz",
      "foo/bar/baz.gz",
      "foo/bar/baz.gz?",
      "foo/bar/baz.gz?something"
    )

    schemes.flatMap(s => validUrls.map(s + "://" + _)).foreach { url =>
      withClue(show"URL: $url") {
        inside(Url.parse(url)) {
          case Valid(u) =>
            u.scheme mustBe url.takeWhile(_ != ':')
            u.authority.host.show mustBe "foo"
            u.authority.userInfo mustBe None
            u.authority.port mustBe None
        }

        inside(Url.parse(url)) {
          case Valid(u) =>
            u.scheme mustBe url.takeWhile(_ != ':')
            u.authority.host.show mustBe "foo"
        }
      }
    }
  }

  it should "parse file uris" in {
    inside(Url.parse("file:/bar")) {
      case Valid(u) =>
        u.scheme mustBe "file"
        u.authority.host.show mustBe "localhost"
        u.path.show mustBe "/bar"
    }

    inside(Url.parse("file://bar")) {
      case Valid(u) =>
        u.scheme mustBe "file"
        u.authority.host.show mustBe "localhost"
        u.path.show mustBe "bar"
    }

    inside(Url.parse("file:///bar")) {
      case Valid(u) =>
        u.scheme mustBe "file"
        u.authority.host.show mustBe "localhost"
        u.path.show mustBe "/bar"
    }
  }

  it should "userinfo and port are allowed for standard urls, but not buckets" in {
    val values = List(User("foo"), Password("bar"), Port(8080))
    val cross  = values.flatMap(v => values.map(v -> _))

    val all = show"https://foo:bar@example.com:8080/foo/"
    val allExpected = Url(
      "https",
      Authority(Host.unsafe("example.com"), Some(UserInfo("foo", "bar".some)), blobstore.url.Port.unsafe(8080).some),
      Path("foo/")
    )

    val candidates: List[Option[(String, Url.Plain)]] = cross.map {
      case (User(u), User(_)) =>
        val v   = show"https://$u@example.com/foo/"
        val url = Url("https", Authority(Host.unsafe("example.com"), Some(UserInfo(u, None)), None), Path("foo/"))
        (v -> url).some
      case (User(u), Password(p)) =>
        val v   = show"https://$u:$p@example.com/foo/"
        val url = Url("https", Authority(Host.unsafe("example.com"), Some(UserInfo(u, p.some)), None), Path("foo/"))
        (v -> url).some
      case (User(u), Port(p)) =>
        val v = show"https://$u@example.com:$p/foo/"
        val url = Url(
          "https",
          Authority(Host.unsafe("example.com"), Some(UserInfo(u, None)), blobstore.url.Port.unsafe(p).some),
          Path("foo/")
        )
        (v -> url).some
      case (Port(p), Port(_)) =>
        val v = show"https://example.com:$p/foo/"
        val url =
          Url("https", Authority(Host.unsafe("example.com"), None, blobstore.url.Port.unsafe(p).some), Path("foo/"))
        (v -> url).some
      case _ => None
    }

    ((all, allExpected).some :: candidates).flattenOption.foreach { case (value, expected) =>
      Url.parse(value) mustBe expected.valid[UrlParseError]
      Url.parseF[Try](value) mustBe Success(expected)
      Url.parse(value) mustBe expected.valid
    }
  }

  it should "give correct error messaages" in {
    inside(Url.parse("foo")) {
      case Invalid(e) =>
        val missingScheme = e.toList.collect {
          case m @ MissingScheme(_, _) => m
        }
        missingScheme mustBe List(MissingScheme("foo", None))
    }
  }

  it should "parse to rootless paths by default" in {
    val url = Url.unsafe("https://example.com/foo/bar")
    url.path mustBe a[RootlessPath[_]]
  }

  it should "convert between schemes" in {
    val url    = Url.unsafe("https://example.com/foo/bar")
    val bucket = Hostname.unsafe("foo")
    url.toAzure(bucket).show mustBe "https://foo/foo/bar"
    url.toS3(bucket).show mustBe "s3://foo/foo/bar"
    url.toGcs(bucket).show mustBe "gs://foo/foo/bar"
    url.toSftp(bucket.authority).show mustBe "sftp://foo/foo/bar"
  }
  // scalafix:on
}

object UrlTest {
  sealed trait UrlComponent
  case class User(v: String)     extends UrlComponent
  case class Password(v: String) extends UrlComponent
  case class Port(v: Int)        extends UrlComponent
}
