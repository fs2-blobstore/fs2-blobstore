package blobstore
package gcs

import blobstore.url.{Authority, Path}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.google.cloud.storage.{BlobInfo, StorageClass}
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import fs2.Stream
import weaver.GlobalRead

import scala.jdk.CollectionConverters._

class GcsStoreTest(global: GlobalRead) extends AbstractStoreTest[GcsBlob, GcsStore[IO]](global) {

  // LocalStorageHelper is not thread safe
  override def maxParallelism = 1

  override val scheme: String       = "gs"
  override val authority: Authority = Authority.unsafe("bucket")

  override val fileSystemRoot: Path.Plain = Path("")
  override val testRunRoot: Path.Plain    = Path(testRun.toString)

  override val sharedResource: Resource[IO, TestResource[GcsBlob, GcsStore[IO]]] =
    Resource.pure {
      val gcsStore: GcsStore[IO] = GcsStore[IO](
        LocalStorageHelper.getOptions.getService,
        defaultTrailingSlashFiles = true,
        defaultDirectDownload = false
      )

      TestResource(gcsStore, gcsStore)
    }

  // When creating "folders" in the GCP UI, a zero byte object with the name of the prefix is created
  // TODO: test("list prefix with first object named the same as prefix")

  // Keys with trailing slashes are perfectly legal in GCS.
  // https://cloud.google.com/storage/docs/naming
  test("handle files with trailing / in name") { res =>
    val dir = dirUrl("trailing-slash")
    val url = dir / "file-with-slash/"
    for {
      data    <- randomBytes(25)
      _       <- Stream.emits(data).through(res.store.put(url)).compile.drain
      l1      <- res.store.listAll(dir)
      content <- res.store.get(url, 128).compile.to(Array)
      _       <- res.store.remove(url)
      l2      <- res.store.listAll(dir)
    } yield {
      expect.all(
        data sameElements content,
        l1.size == 1,
        l2.isEmpty
      ) and l1.map(url =>
        expect.all(
          url.path.fileName.contains("file-with-slash/"),
          !url.path.isDir
        )
      ).combineAll
    }

  }

  test("expose underlying metadata") { (res, log) =>
    val dir = dirUrl("expose-underlying")
    for {
      (url, _) <- writeRandomFile(res.store, log)(dir)
      l        <- res.store.listAll(url)
    } yield {

      // Note: LocalStorageHelper doesn't automatically set other fields like storageClass, md5, etc.
      l.map { u => expect(u.path.representation.blob.getGeneration == 1L) }.combineAll
    }
  }

  test("set underlying metadata on write using typed path") { res =>
    val dir = dirUrl("set-underlying")
    val url = dir / "file.bin"

    val ct = "application/octet-stream"
    val sc = StorageClass.NEARLINE
    val blobInfo = BlobInfo
      .newBuilder(url.authority.show, url.path.show)
      .setContentType(ct)
      .setStorageClass(sc)
      .setMetadata(Map("key" -> "value").asJava)
      .build()

    for {
      data <- randomBytes(25)
      _    <- Stream.emits(data).through(res.extra.put(url.path.as(GcsBlob(blobInfo)), Nil)).compile.drain
      l    <- res.store.listAll(url)
    } yield {
      l.map { u =>
        expect.all(
          u.path.representation.blob.getContentType == ct,
          u.path.representation.blob.getStorageClass == sc,
          u.path.representation.blob.getMetadata.get("key") == "value"
        )
      }.combineAll
    }
  }

  test("direct download") { (res, log) =>
    val dir = dirUrl("direct-download")
    for {
      (url, original) <- writeRandomFile(res.store, log)(dir)
      content         <- res.extra.getUnderlying(url, 4096, direct = true).compile.to(Array)
    } yield {
      expect(original sameElements content)
    }
  }

  test("resolve type of storage class") { res =>
    res.store.listAll(dirUrl("storage-class")).map { l =>
      l.map { u =>
        val sc: Option[StorageClass] = u.path.storageClass
        expect(sc.isEmpty)
      }.combineAll
    }
  }
}
