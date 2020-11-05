package blobstore.url

import cats.data.Validated.Valid
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import cats.syntax.all._
import cats.instances.string._
import org.scalatest.Inside

class UrlParserTest extends AnyFlatSpec with Matchers with Inside {
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
       "foo/bar/baz.gz?something",
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
      case Valid(u) => u.scheme mustBe "file"
      u.authority.host.show mustBe "localhost"
      u.path.show mustBe "/bar"
    }

    inside(Url.standard("file://bar")) {
      case Valid(u) => u.scheme mustBe "file"
      u.authority.host.show mustBe "localhost"
      u.path.show mustBe "bar"
    }

    inside(Url.standard("file:///bar")) {
      case Valid(u) => u.scheme mustBe "file"
      u.authority.host.show mustBe "localhost"
      u.path.show mustBe "/bar"
    }
  }

  it should "userinfo and port are allowed for standard urls, but not buckets" is pending
}
