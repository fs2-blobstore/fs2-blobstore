package blobstore.s3

import java.time.Instant
import java.util.Date

import blobstore.BasePath
import cats.data.Chain
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class S3PathTest extends AnyFlatSpec with Matchers {
  behavior of "S3Path"

  it should "be converted to BasePath on all changes except no-op" in {

    val summary = {
      val s = new S3ObjectSummary()
      s.setBucketName("root")
      s.setKey("subdir/filename")
      s.setSize(20)
      s.setLastModified(Date.from(Instant.EPOCH))
      s
    }
    val s3File = S3Path.S3File(summary, None)

    val s3Path = new S3Path(Right(s3File), knownSize = true)

    s3Path.root must contain("root")
    s3Path.pathFromRoot mustBe Chain("subdir")
    s3Path.fileName must contain("filename")
    s3Path.size must contain(20)
    s3Path.isDir must contain(false)
    s3Path.lastModified must contain(Instant.EPOCH)

    s3Path.withRoot(Some("root")) mustBe a[S3Path]
    s3Path.withRoot(Some("root")) mustBe theSameInstanceAs(s3Path)
    s3Path.withPathFromRoot(Chain("subdir")) mustBe a[S3Path]
    s3Path.withPathFromRoot(Chain("subdir")) mustBe theSameInstanceAs(s3Path)
    s3Path.withFileName(Some("filename")) mustBe a[S3Path]
    s3Path.withFileName(Some("filename")) mustBe theSameInstanceAs(s3Path)
    s3Path.withSize(Some(20)) mustBe a[S3Path]
    s3Path.withSize(Some(20)) mustBe theSameInstanceAs(s3Path)
    s3Path.withIsDir(Some(false)) mustBe a[S3Path]
    s3Path.withIsDir(Some(false)) mustBe theSameInstanceAs(s3Path)
    s3Path.withLastModified(Some(Instant.EPOCH)) mustBe a[S3Path]
    s3Path.withLastModified(Some(Instant.EPOCH)) mustBe theSameInstanceAs(s3Path)

    s3Path.withRoot(Some("different-root")) mustBe BasePath(
      Some("different-root"),
      Chain("subdir"),
      Some("filename"),
      None,
      None,
      None
    )
    s3Path.withPathFromRoot(Chain("different", "subdir")) mustBe BasePath(
      Some("root"),
      Chain("different", "subdir"),
      Some("filename"),
      None,
      None,
      None
    )
    s3Path.withFileName(Some("different-filename")) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("different-filename"),
      None,
      None,
      None
    )
    s3Path.withSize(Some(10)) mustBe BasePath(Some("root"), Chain("subdir"), Some("filename"), Some(10), None, None)
    s3Path.withIsDir(Some(true)) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      None,
      Some(true),
      None
    )
    s3Path.withLastModified(Some(Instant.MAX)) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      None,
      None,
      Some(Instant.MAX)
    )

    s3Path.withRoot(Some("different-root"), reset = false) mustBe BasePath(
      Some("different-root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(Instant.EPOCH)
    )
    s3Path.withPathFromRoot(Chain("different", "subdir"), reset = false) mustBe BasePath(
      Some("root"),
      Chain("different", "subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(Instant.EPOCH)
    )
    s3Path.withFileName(Some("different-filename"), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("different-filename"),
      Some(20),
      Some(false),
      Some(Instant.EPOCH)
    )
    s3Path.withSize(Some(10), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(10),
      Some(false),
      Some(Instant.EPOCH)
    )
    s3Path.withIsDir(Some(true), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(true),
      Some(Instant.EPOCH)
    )
    s3Path.withLastModified(Some(Instant.MAX), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(Instant.MAX)
    )
  }
}
