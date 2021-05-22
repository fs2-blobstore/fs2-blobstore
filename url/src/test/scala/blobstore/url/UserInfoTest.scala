package blobstore.url

import cats.syntax.all._
import weaver.FunSuite

object UserInfoTest extends FunSuite {

  test("mask password") {
    val ui = UserInfo("foo", Some("bar"))
    expect.all(
      ui.show == ui.toString,
      ui.show == "foo:*****",
      ui.toStringWithPassword == "foo:bar"
    )
  }

  test("render correctly without password") {
    val ui = UserInfo("foo", None)
    expect.all(
      ui.show == ui.toString,
      ui.show == "foo",
      ui.toStringWithPassword == "foo"
    )
  }
}
