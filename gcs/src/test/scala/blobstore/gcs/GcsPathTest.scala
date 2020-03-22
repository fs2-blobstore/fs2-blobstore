package blobstore.gcs

import java.math.BigInteger
import java.time.Instant

import blobstore.BasePath
import cats.data.Chain
import com.google.api.client.util.DateTime
import com.google.api.services.storage.model.StorageObject
import com.google.cloud.storage.BlobInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class GcsPathTest extends AnyFlatSpec with Matchers {
  behavior of "GcsPath"

  it should "be converted to BasePath on all changes except no-op" in {
    @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
    val blobInfo: BlobInfo = {
      val storageObject = new StorageObject()
      storageObject.setBucket("root")
      storageObject.setName("subdir/filename")
      storageObject.setSize(new BigInteger("20"))
      storageObject.setUpdated(new DateTime(Instant.EPOCH.toEpochMilli))
      val fromPb = classOf[BlobInfo].getDeclaredMethod("fromPb", classOf[StorageObject])
      fromPb.setAccessible(true)
      fromPb.invoke(classOf[BlobInfo], storageObject).asInstanceOf[BlobInfo]
    }

    val gcsPath = new GcsPath(blobInfo)

    gcsPath.root must contain("root")
    gcsPath.pathFromRoot mustBe Chain("subdir")
    gcsPath.fileName must contain("filename")
    gcsPath.size must contain(20)
    gcsPath.isDir must contain(false)
    gcsPath.lastModified must contain(Instant.EPOCH)

    gcsPath.withRoot(Some("root")) mustBe a[GcsPath]
    gcsPath.withRoot(Some("root")) mustBe theSameInstanceAs(gcsPath)
    gcsPath.withPathFromRoot(Chain("subdir")) mustBe a[GcsPath]
    gcsPath.withPathFromRoot(Chain("subdir")) mustBe theSameInstanceAs(gcsPath)
    gcsPath.withFileName(Some("filename")) mustBe a[GcsPath]
    gcsPath.withFileName(Some("filename")) mustBe theSameInstanceAs(gcsPath)
    gcsPath.withSize(Some(20)) mustBe a[GcsPath]
    gcsPath.withSize(Some(20)) mustBe theSameInstanceAs(gcsPath)
    gcsPath.withIsDir(Some(false)) mustBe a[GcsPath]
    gcsPath.withIsDir(Some(false)) mustBe theSameInstanceAs(gcsPath)
    gcsPath.withLastModified(Some(Instant.EPOCH)) mustBe a[GcsPath]
    gcsPath.withLastModified(Some(Instant.EPOCH)) mustBe theSameInstanceAs(gcsPath)

    gcsPath.withRoot(Some("different-root")) mustBe BasePath(
      Some("different-root"),
      Chain("subdir"),
      Some("filename"),
      None,
      None,
      None
    )
    gcsPath.withPathFromRoot(Chain("different", "subdir")) mustBe BasePath(
      Some("root"),
      Chain("different", "subdir"),
      Some("filename"),
      None,
      None,
      None
    )
    gcsPath.withFileName(Some("different-filename")) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("different-filename"),
      None,
      None,
      None
    )
    gcsPath.withSize(Some(10)) mustBe BasePath(Some("root"), Chain("subdir"), Some("filename"), Some(10), None, None)
    gcsPath.withIsDir(Some(true)) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      None,
      Some(true),
      None
    )
    gcsPath.withLastModified(Some(Instant.MAX)) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      None,
      None,
      Some(Instant.MAX)
    )

    gcsPath.withRoot(Some("different-root"), reset = false) mustBe BasePath(
      Some("different-root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(Instant.EPOCH)
    )
    gcsPath.withPathFromRoot(Chain("different", "subdir"), reset = false) mustBe BasePath(
      Some("root"),
      Chain("different", "subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(Instant.EPOCH)
    )
    gcsPath.withFileName(Some("different-filename"), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("different-filename"),
      Some(20),
      Some(false),
      Some(Instant.EPOCH)
    )
    gcsPath.withSize(Some(10), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(10),
      Some(false),
      Some(Instant.EPOCH)
    )
    gcsPath.withIsDir(Some(true), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(true),
      Some(Instant.EPOCH)
    )
    gcsPath.withLastModified(Some(Instant.MAX), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(Instant.MAX)
    )
  }
}
