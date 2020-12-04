package blobstore.url

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import cats.syntax.all._

class UserInfoTest extends AnyFlatSpec with Matchers {
  behavior of "Userinfo"

  it should "mask password" in {
    val ui = UserInfo("foo", Some("bar"))
    ui.show mustBe ui.toString
    ui.show mustBe "foo:*****"
    ui.toStringWithPassword mustBe "foo:bar"
  }

  it should "render correctly without password" in {
    val ui = UserInfo("foo", None)
    ui.show mustBe ui.toString
    ui.show mustBe "foo"
    ui.toStringWithPassword mustBe "foo"
  }
}
