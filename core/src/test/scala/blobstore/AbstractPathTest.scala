package blobstore

import java.time.Instant

import cats.data.Chain
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers

import scala.reflect.ClassTag

abstract class AbstractPathTest[T <: Path: ClassTag] extends AnyFlatSpecLike with Matchers {
  val root: String          = "root"
  val dir: String           = "subdir"
  val fileName: String      = "filename"
  val fileSize: Long        = 20L
  val lastModified: Instant = Instant.EPOCH

  val concretePath: T

  it should "be converted to BasePath on all changes except no-op" in {

    concretePath.root must contain(root)
    concretePath.pathFromRoot mustBe Chain(dir)
    concretePath.fileName must contain(fileName)
    concretePath.size must contain(fileSize)
    concretePath.isDir must contain(false)
    concretePath.lastModified must contain(lastModified)

    concretePath.withRoot(Some("root")) mustBe a[T]
    concretePath.withRoot(Some("root")) mustBe theSameInstanceAs(concretePath)
    concretePath.withPathFromRoot(Chain("subdir")) mustBe a[T]
    concretePath.withPathFromRoot(Chain("subdir")) mustBe theSameInstanceAs(concretePath)
    concretePath.withFileName(Some("filename")) mustBe a[T]
    concretePath.withFileName(Some("filename")) mustBe theSameInstanceAs(concretePath)
    concretePath.withSize(Some(20)) mustBe a[T]
    concretePath.withSize(Some(20)) mustBe theSameInstanceAs(concretePath)
    concretePath.withIsDir(Some(false)) mustBe a[T]
    concretePath.withIsDir(Some(false)) mustBe theSameInstanceAs(concretePath)
    concretePath.withLastModified(Some(Instant.EPOCH)) mustBe a[T]
    concretePath.withLastModified(Some(Instant.EPOCH)) mustBe theSameInstanceAs(concretePath)

    concretePath.withRoot(Some("different-root")) mustBe BasePath(
      Some("different-root"),
      Chain("subdir"),
      Some("filename"),
      None,
      None,
      None
    )
    concretePath.withPathFromRoot(Chain("different", "subdir")) mustBe BasePath(
      Some("root"),
      Chain("different", "subdir"),
      Some("filename"),
      None,
      None,
      None
    )
    concretePath.withFileName(Some("different-filename")) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("different-filename"),
      None,
      None,
      None
    )
    concretePath.withSize(Some(10)) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(10),
      None,
      None
    )
    concretePath.withIsDir(Some(true)) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      None,
      Some(true),
      None
    )
    concretePath.withLastModified(Some(Instant.MAX)) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      None,
      None,
      Some(Instant.MAX)
    )

    concretePath.withRoot(Some("different-root"), reset = false) mustBe BasePath(
      Some("different-root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(Instant.EPOCH)
    )
    concretePath.withPathFromRoot(Chain("different", "subdir"), reset = false) mustBe BasePath(
      Some("root"),
      Chain("different", "subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(Instant.EPOCH)
    )
    concretePath.withFileName(Some("different-filename"), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("different-filename"),
      Some(20),
      Some(false),
      Some(Instant.EPOCH)
    )
    concretePath.withSize(Some(10), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(10),
      Some(false),
      Some(Instant.EPOCH)
    )
    concretePath.withIsDir(Some(true), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(true),
      Some(Instant.EPOCH)
    )
    concretePath.withLastModified(Some(Instant.MAX), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(Instant.MAX)
    )
  }
}
