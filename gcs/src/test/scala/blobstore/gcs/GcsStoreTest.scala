package blobstore
package gcs

import implicits._
import cats.effect.IO
import fs2.Stream
import com.google.cloud.storage.{BlobInfo, StorageClass}
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import org.scalatest.{Assertion, Inside}

import scala.jdk.CollectionConverters._

class GcsStoreTest extends AbstractStoreTest with Inside {
  val gcsStore: GcsStore[IO] = GcsStore[IO](
    LocalStorageHelper.getOptions.getService,
    blocker,
    defaultTrailingSlashFiles = true,
    defaultDirectDownload = false
  )

  override val store: Store[IO] = gcsStore

  override val root: String = "bucket"

  behavior of "GcsStore"

  // Keys with trailing slashes are perfectly legal in GCS.
  // https://cloud.google.com/storage/docs/naming
  it should "handle files with trailing / in name" in {
    val dir: Path = dirPath("trailing-slash")
    val filePath  = dir / "file-with-slash/"

    store.put("test", filePath).unsafeRunSync()

    // Note
    val entities = store.list(dir).compile.toList.unsafeRunSync()
    entities.foreach { listedPath =>
      listedPath.fileName mustBe Some("file-with-slash/")
      listedPath.isDir mustBe Some(false)
    }

    store.getContents(filePath).unsafeRunSync() mustBe "test"

    store.remove(filePath).unsafeRunSync()

    store.list(dir).compile.toList.unsafeRunSync() mustBe empty
  }

  it should "expose underlying metadata" in {
    val dir  = dirPath("expose-underlying")
    val path = writeFile(store, dir)("abc.txt")

    val entities = store.list(path).map(GcsPath.narrow).unNone.compile.toList.unsafeRunSync()

    entities.foreach { gcsPath =>
      // Note: LocalStorageHelper doesn't automatically set other fields like storageClass, md5, etc.
      gcsPath.blobInfo.getGeneration mustBe 1
    }
  }

  it should "set underlying metadata on write" in {
    val ct = "text/plain"
    val sc = StorageClass.NEARLINE
    val blobInfo = BlobInfo
      .newBuilder(root, s"test-$testRun/set-underlying/file")
      .setContentType(ct)
      .setStorageClass(sc)
      .setMetadata(Map("key" -> "value").asJava)
      .build()
    val filePath = new GcsPath(blobInfo)
    Stream("data".getBytes.toIndexedSeq: _*).through(store.put(filePath)).compile.drain.unsafeRunSync()
    val entities = store.list(filePath).map(GcsPath.narrow).unNone.compile.toList.unsafeRunSync()

    entities.foreach { gcsPath =>
      gcsPath.blobInfo.getContentType mustBe ct
      gcsPath.blobInfo.getStorageClass mustBe sc
      gcsPath.blobInfo.getMetadata must contain key "key"
      gcsPath.blobInfo.getMetadata.get("key") mustBe "value"
    }
  }

  it should "fail trying to reference path with no root" in {
    val path = Path("s3://bucket/folder/file")
    inside[Option[Path], Assertion](Path.fromString("/folder/file.txt", forceRoot = false)) {
      case Some(pathNoRoot) =>
        the[IllegalArgumentException] thrownBy store
          .get(pathNoRoot, 10)
          .compile
          .drain
          .unsafeRunSync() must have message s"Unable to read '$pathNoRoot' - root (bucket) is required to reference blobs in GCS"
        the[IllegalArgumentException] thrownBy Stream.empty
          .through(store.put(pathNoRoot))
          .compile
          .drain
          .unsafeRunSync() must have message s"Unable to write to '$pathNoRoot' - root (bucket) is required to reference blobs in GCS"
        the[IllegalArgumentException] thrownBy store
          .list(pathNoRoot)
          .compile
          .drain
          .unsafeRunSync() must have message s"Unable to list '$pathNoRoot' - root (bucket) is required to reference blobs in GCS"
        the[IllegalArgumentException] thrownBy store
          .remove(pathNoRoot)
          .unsafeRunSync() must have message s"Unable to remove '$pathNoRoot' - root (bucket) is required to reference blobs in GCS"
        the[IllegalArgumentException] thrownBy store
          .copy(pathNoRoot, path)
          .unsafeRunSync() must have message s"Wrong src '$pathNoRoot' - root (bucket) is required to reference blobs in GCS"
        the[IllegalArgumentException] thrownBy store
          .copy(path, pathNoRoot)
          .unsafeRunSync() must have message s"Wrong dst '$pathNoRoot' - root (bucket) is required to reference blobs in GCS"
    }
  }

  it should "support direct download" in {
    val dir: Path = dirPath("direct-download")
    val filename  = s"test-${System.currentTimeMillis}.txt"
    val path      = writeFile(store, dir)(filename)

    val content = gcsStore
      .getUnderlying(path, 4096, direct = true, maxChunksInFlight = None)
      .through(fs2.text.utf8Decode)
      .compile
      .toList
      .map(_.mkString)

    content.unsafeRunSync() must be(contents(filename))
  }
}
