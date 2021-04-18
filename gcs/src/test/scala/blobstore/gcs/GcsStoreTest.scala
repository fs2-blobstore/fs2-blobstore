package blobstore
package gcs

import blobstore.url.{Authority, Path, Url}
import blobstore.url.Path.Plain
import blobstore.Store
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.google.cloud.storage.{BlobInfo, StorageClass}
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import fs2.Stream
import org.scalatest.Inside

import scala.jdk.CollectionConverters._

class GcsStoreTest extends AbstractStoreTest[GcsBlob] with Inside {

  override val scheme: String        = "gs"
  override val authority: Authority  = Authority.unsafe("bucket")
  override val fileSystemRoot: Plain = Path("")

  val gcsStore: GcsStore[IO] = GcsStore[IO](
    LocalStorageHelper.getOptions.getService,
    defaultTrailingSlashFiles = true,
    defaultDirectDownload = false
  )

  override def mkStore(): GcsStore[IO] = gcsStore

  behavior of "GcsStore"

  it should "pick up correct storage class" in {
    val dir     = dirUrl("storage-class")
    val fileUrl = dir / "file"

    store.putContent(fileUrl, "test").unsafeRunSync()

    store.list(fileUrl).map { u =>
      u.path.storageClass mustBe None // not supported by gcs test lib
    }.compile.lastOrError.unsafeRunSync()

    val storeGeneric: Store.Generic[IO] = store

    storeGeneric.list(fileUrl).map { u =>
      u.path.storageClass mustBe None
    }.compile.lastOrError.unsafeRunSync()
  }

  // When creating "folders" in the GCP UI, a zero byte object with the name of the prefix is created
  it should "list prefix with first object named the same as prefix" is pending

  // Keys with trailing slashes are perfectly legal in GCS.
  // https://cloud.google.com/storage/docs/naming
  it should "handle files with trailing / in name" in {
    val dir      = dirUrl("trailing-slash")
    val filePath = dir / "file-with-slash/"

    store.putContent(filePath, "test").unsafeRunSync()

    // Note
    val entities = store.list(dir).compile.toList.unsafeRunSync()
    entities must not be Nil

    entities.foreach { listedUrl =>
      listedUrl.path.fileName mustBe Some("file-with-slash/")
      listedUrl.path.isDir mustBe false
    }

    store.get(filePath, 4096).through(fs2.text.utf8Decode).compile.string.unsafeRunSync() mustBe "test"

    store.remove(filePath).unsafeRunSync()

    store.list(dir).compile.toList.unsafeRunSync() mustBe Nil
  }

  it should "expose underlying metadata" in {
    val dir  = dirUrl("expose-underlying")
    val path = writeFile(store, dir)("abc.txt")

    val entities = store.list(path).compile.toList.unsafeRunSync()

    entities.foreach { gcsUrl =>
      // Note: LocalStorageHelper doesn't automatically set other fields like storageClass, md5, etc.
      gcsUrl.path.representation.blob.getGeneration mustBe 1
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

    entities.foreach { gcsUrl =>
      gcsUrl.path.representation.blob.getContentType mustBe ct
      gcsUrl.path.representation.blob.getStorageClass mustBe sc
      gcsUrl.path.representation.blob.getMetadata must contain key "key"
      gcsUrl.path.representation.blob.getMetadata.get("key") mustBe "value"
    }
  }

  it should "support direct download" in {
    val dir      = dirUrl("direct-download")
    val filename = s"test-${System.currentTimeMillis}.txt"
    val path     = writeFile(store, dir)(filename)

    val content = gcsStore
      .getUnderlying(path, 4096, direct = true)
      .through(fs2.text.utf8Decode)
      .compile
      .toList
      .map(_.mkString)

    content.unsafeRunSync() must be(contents(filename))
  }

  it should "resolve type of storage class" in {
    gcsStore.list(dirUrl("foo")).map { u =>
      val sc: Option[StorageClass] = u.path.storageClass
      sc mustBe None
    }
  }
}
