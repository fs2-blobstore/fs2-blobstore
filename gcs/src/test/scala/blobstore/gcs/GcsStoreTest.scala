package blobstore
package gcs

import blobstore.url.{Path, Url}
import blobstore.url.Authority.Bucket
import blobstore.url.Path.Plain
import cats.effect.IO
import cats.syntax.all._
import com.google.cloud.storage.{BlobInfo, StorageClass}
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import fs2.Stream
import org.scalatest.Inside

import scala.jdk.CollectionConverters._

class GcsStoreTest extends AbstractStoreTest[Bucket, GcsBlob] with Inside {

  override val scheme: String        = "gs"
  override val authority: Bucket     = Bucket.unsafe("bucket")
  override val fileSystemRoot: Plain = Path("")

  val gcsStore: GcsStore[IO] = GcsStore[IO](
    LocalStorageHelper.getOptions.getService,
    blocker,
    defaultTrailingSlashFiles = true,
    defaultDirectDownload = false
  )

  override def mkStore(): GcsStore[IO] = gcsStore

  behavior of "GcsStore"

  // When creating "folders" in the GCP UI, a zero byte object with the name of the prefix is created
  it should "list prefix with first object named the same as prefix" is pending

  // Keys with trailing slashes are perfectly legal in GCS.
  // https://cloud.google.com/storage/docs/naming
  it should "handle files with trailing / in name" in {
    val dir: Url[Bucket] = dirUrl("trailing-slash")
    val filePath         = dir / "file-with-slash/"

    store.put("test", filePath).compile.drain.unsafeRunSync()

    // Note
    val entities = store.list(dir).compile.toList.unsafeRunSync()
    entities must not be Nil

    entities.foreach { listedPath =>
      listedPath.fileName mustBe Some("file-with-slash/")
      listedPath.isDir mustBe false
    }

    store.get(filePath, 4096).through(fs2.text.utf8Decode).compile.string.unsafeRunSync() mustBe "test"

    store.remove(filePath).unsafeRunSync()

    store.list(dir).compile.toList.unsafeRunSync() mustBe Nil
  }

  it should "expose underlying metadata" in {
    val dir  = dirUrl("expose-underlying")
    val path = writeFile(store, dir.path)("abc.txt")

    val entities = store.list(path).compile.toList.unsafeRunSync()

    entities.foreach { gcsPath =>
      // Note: LocalStorageHelper doesn't automatically set other fields like storageClass, md5, etc.
      gcsPath.representation.blob.getGeneration mustBe 1
    }
  }

  it should "set underlying metadata on write" in {
    val ct  = "text/plain"
    val sc  = StorageClass.NEARLINE
    val url = Url("gs", authority, Path(s"test-$testRun/set-underlying/file"))

    val blobInfo = BlobInfo
      .newBuilder(url.authority.show, url.path.show)
      .setContentType(ct)
      .setStorageClass(sc)
      .setMetadata(Map("key" -> "value").asJava)
      .build()

    Stream("data".getBytes.toIndexedSeq: _*).through(
      gcsStore.put(url.path.as(GcsBlob(blobInfo)), List.empty)
    ).compile.drain.unsafeRunSync()
    val entities = store.list(url).compile.toList.unsafeRunSync()

    entities.foreach { gcsPath =>
      gcsPath.representation.blob.getContentType mustBe ct
      gcsPath.representation.blob.getStorageClass mustBe sc
      gcsPath.representation.blob.getMetadata must contain key "key"
      gcsPath.representation.blob.getMetadata.get("key") mustBe "value"
    }
  }

  it should "support direct download" in {
    val dir: Url[Bucket] = dirUrl("direct-download")
    val filename         = s"test-${System.currentTimeMillis}.txt"
    val path             = writeFile(store, dir.path)(filename)

    val content = gcsStore
      .getUnderlying(path, 4096, direct = true, maxChunksInFlight = None)
      .through(fs2.text.utf8Decode)
      .compile
      .toList
      .map(_.mkString)

    content.unsafeRunSync() must be(contents(filename))
  }

  it should "resolve type of storage class" in {
    gcsStore.list(dirUrl("foo")).map { path =>
      val sc: Option[StorageClass] = path.storageClass
      sc mustBe None
    }
  }
}
