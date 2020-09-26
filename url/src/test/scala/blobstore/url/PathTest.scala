package blobstore.url

import blobstore.url.Path.{AbsolutePath, RootlessPath}
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

    absolutePaths.map(Path.apply).zipWithIndex.foreach { case (p,i) =>
      p mustBe a[AbsolutePath[String]]
      p.value mustBe absolutePaths(i)
    }

    rootlessPaths.map(Path.apply).zipWithIndex.foreach { case (p, i) =>
      p mustBe a[RootlessPath[String]]
      p.value mustBe rootlessPaths(i)
    }
  }

  it should "compose paths" in {
    Path("/foo") / "bar" mustBe Path("/foo/bar")
    Path("/foo/") / "bar" mustBe Path("/foo/bar")
    Path("/foo") / "/bar" mustBe Path("/foo//bar")
    Path("/foo") / "bar/" mustBe Path("/foo/bar/")
    Path("/foo") / "bar//" mustBe Path("/foo/bar//")
    Path("/foo") `//` "bar" mustBe Path("/foo/bar/")
    Path("/foo/") `//` "bar" mustBe Path("/foo/bar/")
    Path("/foo//") `//` "bar" mustBe Path("/foo//bar/")
    Path("/foo//") `//` "/bar" mustBe Path("/foo///bar/")
  }

  it should "create absolute paths" in {
    val validPaths = List(
      "/foo",
      "//foo",
      "/foo/bar",
      "/foo/bar/",
      "/foo/bar//",
    )

    val invalidPaths = List(
      "",
      "foo",
      "foo/bar",
      "foo/bar/",
      "foo/bar//",
    )

    validPaths.map(AbsolutePath.parse).zipWithIndex.foreach {
      case (p, i) => inside(p) {
        case Some(path) => path.value mustBe validPaths(i)
      }
    }

    invalidPaths.map(AbsolutePath.parse).foreach(p => p mustBe None)
    invalidPaths.map(AbsolutePath.createFrom).zipWithIndex.foreach {
      case (p, i) => p.value mustBe "/" + invalidPaths(i)
    }
  }
}
