package blobstore.url

import blobstore.url.exception.UrlParseError
import blobstore.url.exception.UrlParseError.MissingScheme
import blobstore.url.Path.RootlessPath
import blobstore.url.exception.AuthorityParseError.MissingHost
import cats.data.NonEmptyChain
import cats.syntax.all._
import weaver.FunSuite

import scala.util.{Success, Try}

object UrlTest extends FunSuite {

  sealed trait UrlComponent
  case class User(v: String)     extends UrlComponent
  case class Password(v: String) extends UrlComponent

  test("compose") {
    val fileLike = "https://foo.example.com"

    val parsed = Url.parse(fileLike)

    expect(parsed.isValid) and parsed.map { url =>
      expect.all(
        (url / "foo").show == "https://foo.example.com/foo",
        (url / "/foo").show == "https://foo.example.com/foo",
        (url / "//foo").show == "https://foo.example.com//foo",
        (url / Some("foo")).show == "https://foo.example.com/foo",
        (url / Path("foo")).show == "https://foo.example.com/foo",
        (url / Path("/foo")).show == "https://foo.example.com/foo",
        (url `//` "foo").show == "https://foo.example.com/foo/",
        (url / "foo").withPath(Path("bar/baz/")).show == "https://foo.example.com/bar/baz/",
        (url / "foo").withAuthority(Authority.unsafe("bar.example.com")).show == "https://bar.example.com/foo"
      )
    }.combineAll
  }

  test("render correctly with toString") {
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

    val e1 = expect.all(
      url1.toString == "https://foo@foo.com:8080/foo",
      url1.toStringMasked == "https://foo:*****@foo.com:8080/foo",
      url1.toStringWithPassword == "https://foo:bar@foo.com:8080/foo"
    )

    val e2 = expect.all(
      url2.toString == "https://foo@foo.com:8080/foo",
      url2.toStringMasked == "https://foo@foo.com:8080/foo",
      url2.toStringWithPassword == "https://foo@foo.com:8080/foo"
    )

    val e3 = expect.all(
      url3.toString == "https://foo.com:8080/foo",
      url3.toStringMasked == "https://foo.com:8080/foo",
      url3.toStringWithPassword == "https://foo.com:8080/foo"
    )

    val e4 = expect.all(
      url4.toString == "https://foo.com/foo",
      url4.toStringMasked == "https://foo.com/foo",
      url4.toStringWithPassword == "https://foo.com/foo"
    )

    e1 and e2 and e3 and e4
  }

  test("parse valid urls") {
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

    schemes.flatMap(s => validUrls.map(s ++ "://" ++ _)).map { s =>
      val parsed = Url.parse(s)
      expect(parsed.isValid) and parsed.map { url =>
        expect.all(
          url.scheme == s.takeWhile(_ != ':'),
          url.authority.host.show == "foo",
          url.authority.userInfo.isEmpty,
          url.authority.port.isEmpty
        )
      }.combineAll
    }.combineAll
  }

  test("parse file uris") {
    val p1 = Url.parse("file:/bar")
    val e1 = expect(p1.isValid) and p1.map { url =>
      expect.all(
        url.scheme == "file",
        url.authority.host.show == "localhost",
        url.path.show == "/bar"
      )
    }.combineAll

    val p2 = Url.parse("file://bar")
    val e2 = expect(p2.isValid) and p2.map { url =>
      expect.all(
        url.scheme == "file",
        url.authority.host.show == "localhost",
        url.path.show == "bar"
      )
    }.combineAll

    val p3 = Url.parse("file:///bar")
    val e3 = expect(p3.isValid) and p3.map { url =>
      expect.all(
        url.scheme == "file",
        url.authority.host.show == "localhost",
        url.path.show == "/bar"
      )
    }.combineAll

    e1 and e2 and e3
  }

  // scalafix:off
  test("userinfo and port are allowed for standard urls, but not buckets") {
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
          Authority(Host.unsafe("example.com"), Some(UserInfo(u, None)), Port.unsafe(p).some),
          Path("foo/")
        )
        (v -> url).some
      case (Port(p), Port(_)) =>
        val v = show"https://example.com:$p/foo/"
        val url =
          Url("https", Authority(Host.unsafe("example.com"), None, Port.unsafe(p).some), Path("foo/"))
        (v -> url).some
      case _ => None
    }

    ((all, allExpected).some :: candidates).flattenOption.map { case (value, expected) =>
      expect.all(
        Url.parse(value) == expected.valid[UrlParseError],
        Url.parseF[Try](value) == Success(expected),
        Url.unsafe(value) == expected
      )
    }.combineAll
  }

  test("give correct error messaages") {
    expect {
      Url.parse("foo") == NonEmptyChain.of(MissingScheme("foo", None), MissingHost("foo")).invalid
    }
  }
  // scalafix:on

  test("parse to rootless paths by default") {
    val url = Url.unsafe("https://example.com/foo/bar")
    expect(url.path match {
      case _: RootlessPath[_] => true
      case _                  => false
    })
  }

  test("convert between schemes") {
    val url    = Url.unsafe("https://example.com/foo/bar")
    val bucket = Hostname.unsafe("foo")
    expect.all(
      url.toAzure(bucket).show == "https://foo/foo/bar",
      url.toS3(bucket).show == "s3://foo/foo/bar",
      url.toGcs(bucket).show == "gs://foo/foo/bar",
      url.toSftp(bucket.authority).show == "sftp://foo/foo/bar"
    )
  }

}
