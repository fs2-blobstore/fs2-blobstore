package blobstore.url

import java.nio.file.Paths

import cats.syntax.all._
import blobstore.url.Path.{AbsolutePath, RootlessPath}
import cats.data.NonEmptyChain
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.Inside

class PathTest extends AnyFlatSpec with Matchers with Inside {

  it should "create correct paths" in {
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

    AbsolutePath.createFrom("").show mustBe "/"
    AbsolutePath.parse("") mustBe None

    RootlessPath.createFrom("/").show mustBe ""
    RootlessPath.parse("/") mustBe None

    absolutePaths.foreach { s =>
      val ps = List(Path(s).some, Path.absolute(s), Path.AbsolutePath.createFrom(s).some)
      ps.foreach { p =>
        inside(p) {
          case Some(p) =>
            p mustBe a[AbsolutePath[_]]
            p.value mustBe s
        }
      }
    }

    rootlessPaths.foreach { s =>
      val ps = List(Path(s).some, Path.rootless(s), Path.RootlessPath.createFrom(s).some)
      ps.foreach { p =>
        inside(p) {
          case Some(p) =>
            p mustBe a[RootlessPath[_]]
            p.value mustBe s
        }
      }
    }
  }

  it should "compose paths" in {
    Path("/foo") / "bar" mustBe Path("/foo/bar")
    Path("/foo") / Some("bar") mustBe Path("/foo/bar")
    Path("/foo") / Some("/bar") mustBe Path("/foo/bar")
    Path("/foo") / "bar" mustBe Path("/foo/bar")
    Path("/foo/") / "bar" mustBe Path("/foo/bar")
    Path("/foo") / "/bar" mustBe Path("/foo/bar")
    Path("/foo") / "//bar" mustBe Path("/foo//bar")
    Path("/foo") / "bar/" mustBe Path("/foo/bar/")
    Path("/foo") / "bar//" mustBe Path("/foo/bar//")
    Path("/foo") `//` "bar" mustBe Path("/foo/bar/")
    Path("/foo") `//` Some("bar") mustBe Path("/foo/bar/")
    Path("/foo") `//` Some("/bar") mustBe Path("/foo/bar/")
    Path("/foo/") `//` "bar" mustBe Path("/foo/bar/")
    Path("/foo//") `//` "bar" mustBe Path("/foo//bar/")
    Path("/foo//") `//` "/bar" mustBe Path("/foo//bar/")
    Path("/foo//") `//` "//bar" mustBe Path("/foo///bar/")
  }

  it should "create absolute paths" in {
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

    validPaths.foreach { s =>
      val p = AbsolutePath.parse(s)
      inside(p) {
        case Some(path) => path.value mustBe s
      }
    }

    invalidPaths.map(AbsolutePath.parse).foreach(p => p mustBe None)
    invalidPaths.foreach { s =>
      val p = AbsolutePath.createFrom(s)
      p.value mustBe "/" ++ s
    }
  }

  it should "add segments" in {
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

    noSlash1.show mustBe "/foo/bar/baz"
    noSlash1.segments mustBe NonEmptyChain("foo", "bar", "baz")
    noSlash2.show mustBe "/foo/bar/baz"
    noSlash2.segments mustBe NonEmptyChain("foo", "bar", "baz")

    slashPrefix1.show mustBe "/foo/bar/baz"
    slashPrefix1.segments mustBe NonEmptyChain("foo", "bar", "baz")
    slashPrefix2.show mustBe "/foo/bar/baz"
    slashPrefix2.segments mustBe NonEmptyChain("foo", "bar", "baz")
    slashPrefix3.show mustBe "/foo/bar//baz"
    slashPrefix3.segments mustBe NonEmptyChain("foo", "bar", "", "baz")

    slashSuffix1.show mustBe "/foo/bar/baz/"
    slashSuffix1.segments mustBe NonEmptyChain("foo", "bar", "baz", "")
    slashSuffix2.show mustBe "/foo/bar/baz/"
    slashSuffix2.segments mustBe NonEmptyChain("foo", "bar", "baz", "")
    slashSuffix3.show mustBe "/foo/bar/baz//"
    slashSuffix3.segments mustBe NonEmptyChain("foo", "bar", "baz", "", "")

    multiple1.show mustBe "/foo/bar/baz/bam/"
    multiple1.segments mustBe NonEmptyChain("foo", "bar", "baz", "bam", "")

    multiple2.show mustBe "/foo/bar//baz//bam//"
    multiple2.segments mustBe NonEmptyChain("foo", "bar", "", "baz", "", "bam", "", "")
  }

  it should "convert to nio path" in {
    val absolute = Path("/foo/bar").nioPath
    val relative = Path("foo/bar").nioPath

    absolute mustBe Paths.get("/foo", "bar")
    absolute.toString mustBe "/foo/bar"
    absolute.getNameCount mustBe 2
    relative mustBe Paths.get("foo", "bar")
    relative.toString mustBe "foo/bar"
    relative.getNameCount mustBe 2
  }

  it should "convert between paths" in {
    Path("foo").absolute.show mustBe "/foo"
    Path("/foo").absolute.show mustBe "/foo"
    Path("foo").relative.show mustBe "foo"
    Path("/foo").relative.show mustBe "foo"
  }

  it should "go up" in {
    Path("/foo/bar/baz").up.show mustBe "/foo/bar"
    Path("/foo/bar/baz/").up.show mustBe "/foo/bar"
    Path("/foo").up.show mustBe "/"
    Path("/").up.show mustBe "/"
    Path("").up.show mustBe ""
  }
}
