package blobstore

import blobstore.url.Path

import cats.data.Chain

import cats.syntax.all._
import weaver.FunSuite

object PathTest extends FunSuite {

  test("parse string path to file correctly") {
    val path = Path("some-bucket/path/to/file")

    expect.all(
      path.representation == "some-bucket/path/to/file",
      path.show == "some-bucket/path/to/file",
      path.segments == Chain("some-bucket", "path", "to", "file"),
      path.lastSegment == "file".some,
      path.plain.show == path.show,
      path.plain.representation == path.representation
    )
  }

  test("parse string path to file without prefix correctly") {
    val path = Path("some-bucket/path/to/file")

    expect.all(
      path.representation == "some-bucket/path/to/file",
      path.show == "some-bucket/path/to/file",
      path.segments == Chain("some-bucket", "path", "to", "file"),
      path.lastSegment == "file".some,
      path.plain.show == path.show,
      path.plain.representation == path.representation
    )
  }

  test("parse string path to file stating with / correctly") {
    val path = Path("/some-bucket/path/to/file")

    expect.all(
      path.representation == "/some-bucket/path/to/file",
      path.show == "/some-bucket/path/to/file",
      path.segments == Chain("some-bucket", "path", "to", "file"),
      path.lastSegment == "file".some,
      path.plain.show == path.show,
      path.plain.representation == path.representation
    )
  }

  test("parse string path to dir correctly") {
    val path = Path("some-bucket/path/to/")

    expect.all(
      path.representation == "some-bucket/path/to/",
      path.show == "some-bucket/path/to/",
      path.segments == Chain("some-bucket", "path", "to", ""),
      path.lastSegment == "to/".some,
      path.plain.show == path.show,
      path.plain.representation == path.representation
    )
  }

  test("parse paths with no key") {
    val path = Path("some-bucket")

    expect.all(
      path.representation == "some-bucket",
      path.show == "some-bucket",
      path.segments == Chain("some-bucket"),
      path.lastSegment == "some-bucket".some,
      path.plain.show == path.show,
      path.plain.representation == path.representation
    )
  }

  test("parse empty path") {
    expect(Path("") == Path.empty)
  }

  test("parse paths with spaces") {
    val path = Path("root with spaces/dir with spaces/file with spaces")

    expect.all(
      path.representation == "root with spaces/dir with spaces/file with spaces",
      path.show == "root with spaces/dir with spaces/file with spaces",
      path.segments == Chain("root with spaces", "dir with spaces", "file with spaces"),
      path.lastSegment == "file with spaces".some,
      path.plain.show == path.show,
      path.plain.representation == path.representation
    )
  }

  test("extend a path with no key correctly") {
    val path = Path("some-bucket") / "key"

    expect.all(
      path.representation == "some-bucket/key",
      path.show == "some-bucket/key",
      path.segments == Chain("some-bucket", "key"),
      path.lastSegment == "key".some,
      path.plain.show == path.show,
      path.plain.representation == path.representation
    )
  }

  test("be extractable using unapply") {
    val path  = Path("some-bucket/folder/file").as(())
    val path2 = Path("some-bucket/folder/file")

    val e1 = expect {
      path match {
        case Path(s, _: Unit, segments) =>
          s == "some-bucket/folder/file" &&
            segments == Chain("some-bucket", "folder", "file")
        case _ => false
      }
    }

    val e2 = expect {
      path2 match {
        case Path(s, representation, segments) =>
          s == "some-bucket/folder/file" &&
            representation == "some-bucket/folder/file" &&
            segments == Chain("some-bucket", "folder", "file")
        case _ => false
      }
    }

    e1 and e2
  }
}
