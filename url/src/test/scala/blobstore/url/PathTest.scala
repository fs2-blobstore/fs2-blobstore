package blobstore.url

import java.nio.file.Paths
import cats.syntax.all._
import blobstore.url.Path.{AbsolutePath, RootlessPath}
import cats.data.Chain
import weaver.FunSuite

object PathTest extends FunSuite {

  test("create correct paths") {
    val absolutePaths = List(
      "/",
      "/foo",
      "/foo/bar",
      "/foo/bar/",
      "/foo/bar//",
      "/foo/bar///"
    )

    val rootlessPaths = List(
      "",
      "foo",
      "./foo",
      "../foo/bar",
      "../foo/bar/",
      "../foo/bar//",
      "../foo/bar//"
    )

    val empty = expect(AbsolutePath.createFrom("").show == "/") and expect(AbsolutePath.parse("").isEmpty)

    val slash = expect(RootlessPath.createFrom("/").show == "") and expect(RootlessPath.parse("/").isEmpty)

    val absolute = absolutePaths.flatMap(s =>
      List(Path(s).some, Path.absolute(s), Path.AbsolutePath.createFrom(s).some).map { maybePath =>
        expect {
          maybePath match {
            case Some(p: AbsolutePath[String]) =>
              p.value == s
            case _ => false
          }
        }
      }
    ).combineAll

    val rootless = rootlessPaths.flatMap { s =>
      List(Path(s).some, Path.rootless(s), Path.RootlessPath.createFrom(s).some).map { maybePath =>
        expect {
          maybePath match {
            case Some(p: RootlessPath[String]) =>
              p.value == s
            case _ => false
          }
        }
      }
    }.combineAll

    empty and slash and absolute and rootless
  }

  test("compose paths") {
    expect.all(
      Path("/foo") / "bar" == Path("/foo/bar"),
      Path("/foo") / Some("bar") == Path("/foo/bar"),
      Path("/foo") / Some("/bar") == Path("/foo/bar"),
      Path("/foo") / "bar" == Path("/foo/bar"),
      Path("/foo/") / "bar" == Path("/foo/bar"),
      Path("/foo") / "/bar" == Path("/foo/bar"),
      Path("/foo") / "//bar" == Path("/foo//bar"),
      Path("/foo") / "bar/" == Path("/foo/bar/"),
      Path("/foo") / "bar//" == Path("/foo/bar//"),
      Path("/foo").`//`("bar") == Path("/foo/bar/"),
      Path("/foo").`//`(Some("bar")) == Path("/foo/bar/"),
      Path("/foo").`//`(Some("/bar")) == Path("/foo/bar/"),
      Path("/foo/").`//`("bar") == Path("/foo/bar/"),
      Path("/foo//").`//`("bar") == Path("/foo//bar/"),
      Path("/foo//").`//`("/bar") == Path("/foo//bar/"),
      Path("/foo//").`//`("//bar") == Path("/foo///bar/")
    )
  }

  test("create absolute paths") {
    val validPaths = List(
      "/",
      "/foo",
      "//foo",
      "/foo/bar",
      "/foo/bar/",
      "/foo/bar//"
    )

    val invalidPaths = List(
      "",
      "foo",
      "foo/bar",
      "foo/bar/",
      "foo/bar//"
    )

    val valid = validPaths.map { s =>
      expect {
        AbsolutePath.parse(s) match {
          case Some(p) => p.value == s
          case None    => false
        }
      }
    }.combineAll

    val invalidParse = invalidPaths.map(AbsolutePath.parse).map(p => expect(p.isEmpty)).combineAll

    val invalidCreateFrom = invalidPaths.map { s =>
      val p = AbsolutePath.createFrom(s)
      expect(p.value == s"/$s")
    }.combineAll

    valid and invalidParse and invalidCreateFrom
  }

  test("add segments") {
    val p1 = Path("/foo/bar/")
    val p2 = Path("/foo/bar")

    val noSlash1 = p1.addSegment("baz", ())
    val noSlash2 = p2.addSegment("baz", ())

    val slashPrefix1 = p1.addSegment("/baz", ())
    val slashPrefix2 = p2.addSegment("/baz", ())
    val slashPrefix3 = p2.addSegment("//baz", ())

    val slashSuffix1 = p1.addSegment("baz/", ())
    val slashSuffix2 = p2.addSegment("baz/", ())
    val slashSuffix3 = p2.addSegment("baz//", ())

    val multiple1 = p1.addSegment("/baz/bam/", ())
    val multiple2 = p1.addSegment("//baz//bam//", ())

    expect.all(
      noSlash1.show == "/foo/bar/baz",
      noSlash1.segments == Chain("foo", "bar", "baz"),
      noSlash2.show == "/foo/bar/baz",
      noSlash2.segments == Chain("foo", "bar", "baz"),
      slashPrefix1.show == "/foo/bar/baz",
      slashPrefix1.segments == Chain("foo", "bar", "baz"),
      slashPrefix2.show == "/foo/bar/baz",
      slashPrefix2.segments == Chain("foo", "bar", "baz"),
      slashPrefix3.show == "/foo/bar//baz",
      slashPrefix3.segments == Chain("foo", "bar", "", "baz"),
      slashSuffix1.show == "/foo/bar/baz/",
      slashSuffix1.segments == Chain("foo", "bar", "baz", ""),
      slashSuffix2.show == "/foo/bar/baz/",
      slashSuffix2.segments == Chain("foo", "bar", "baz", ""),
      slashSuffix3.show == "/foo/bar/baz//",
      slashSuffix3.segments == Chain("foo", "bar", "baz", "", ""),
      multiple1.show == "/foo/bar/baz/bam/",
      multiple1.segments == Chain("foo", "bar", "baz", "bam", ""),
      multiple2.show == "/foo/bar//baz//bam//",
      multiple2.segments == Chain("foo", "bar", "", "baz", "", "bam", "", "")
    )

  }

  test("convert to nio path") {
    val absolute = Path("/foo/bar").nioPath
    val relative = Path("foo/bar").nioPath

    expect.all(
      absolute == Paths.get("/foo", "bar"),
      absolute.toString == "/foo/bar",
      absolute.getNameCount == 2,
      relative == Paths.get("foo", "bar"),
      relative.toString == "foo/bar",
      relative.getNameCount == 2
    )
  }

  test("convert between paths") {
    expect.all(
      Path("foo").absolute.show == "/foo",
      Path("/foo").absolute.show == "/foo",
      Path("foo").relative.show == "foo",
      Path("/foo").relative.show == "foo"
    )
  }

  test("go up") {
    expect.all(
      Path("/foo/bar/baz").up.show == "/foo/bar",
      Path("/foo/bar/baz/").up.show == "/foo/bar",
      Path("/foo").up.show == "/",
      Path("/").up.show == "/",
      Path("").up.show == ""
    )
  }
}
