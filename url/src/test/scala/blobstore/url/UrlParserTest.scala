package blobstore.url

import blobstore.url.Authority.Standard
import blobstore.url.exception.UrlParseError
import cats.data.Validated.Valid
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import cats.syntax.all._
import org.scalatest.Inside

import scala.util.{Success, Try}

class UrlParserTest extends AnyFlatSpec with Matchers with Inside {
  import UrlParserTest._

  behavior of "UrlParser"

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
        inside(Url.standard(url)) {
          case Valid(u) =>
            u.scheme mustBe url.takeWhile(_ != ':')
            u.authority.host.show mustBe "foo"
            u.authority.userInfo mustBe None
            u.authority.port mustBe None
        }

        inside(Url.bucket(url)) {
          case Valid(u) =>
            u.scheme mustBe url.takeWhile(_ != ':')
            u.authority.host.show mustBe "foo"
        }
      }
    }
  }

  it should "parse file uris" in {
    inside(Url.standard("file:/bar")) {
      case Valid(u) =>
        u.scheme mustBe "file"
        u.authority.host.show mustBe "localhost"
        u.path.show mustBe "/bar"
    }

    inside(Url.standard("file://bar")) {
      case Valid(u) =>
        u.scheme mustBe "file"
        u.authority.host.show mustBe "localhost"
        u.path.show mustBe "bar"
    }

    inside(Url.standard("file:///bar")) { // scalafix:ok
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
      Standard(Host.unsafe("example.com"), Some(UserInfo("foo", "bar".some)), blobstore.url.Port.unsafe(8080).some),
      Path("/foo/")
    )

    val candidates: List[Option[(String, Url[Standard])]] = cross.map {
      case (User(u), User(_)) =>
        val v   = show"https://$u@example.com/foo/"
        val url = Url("https", Standard(Host.unsafe("example.com"), Some(UserInfo(u, None)), None), Path("/foo/"))
        (v -> url).some
      case (User(u), Password(p)) =>
        val v   = show"https://$u:$p@example.com/foo/"
        val url = Url("https", Standard(Host.unsafe("example.com"), Some(UserInfo(u, p.some)), None), Path("/foo/"))
        (v -> url).some
      case (User(u), Port(p)) =>
        val v = show"https://$u@example.com:$p/foo/"
        val url = Url(
          "https",
          Standard(Host.unsafe("example.com"), Some(UserInfo(u, None)), blobstore.url.Port.unsafe(p).some),
          Path("/foo/")
        )
        (v -> url).some
      case (Port(p), Port(_)) =>
        val v = show"https://example.com:$p/foo/"
        val url =
          Url("https", Standard(Host.unsafe("example.com"), None, blobstore.url.Port.unsafe(p).some), Path("/foo/"))
        (v -> url).some
      case _ => None
    }

    ((all, allExpected).some :: candidates).flattenOption.foreach { case (value, expected) =>
      Url.parse[Standard](value) mustBe expected.valid[UrlParseError]
      Url.parseF[Try, Standard](value) mustBe Success(expected)
      Url.standard(value) mustBe expected.valid

      withClue(show"$value === ${expected.toStringWithPassword}") {
        Url.bucket(value).toEither mustBe a[Left[_, _]]
        Url.bucketF[Either[Throwable, *]](value) mustBe a[Left[_, _]]
        Url.unsafe[Standard](value).authority.toBucket.toEither mustBe a[Left[_, _]]
      }
    }
  }

}

object UrlParserTest {
  sealed trait UrlComponent
  case class User(v: String)     extends UrlComponent
  case class Password(v: String) extends UrlComponent
  case class Port(v: Int)        extends UrlComponent
}
