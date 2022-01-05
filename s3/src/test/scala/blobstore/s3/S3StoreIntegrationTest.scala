package blobstore.s3

import blobstore.{IntegrationTest, Store}
import blobstore.url.{Authority, Path, Url}
import cats.effect.unsafe.implicits.global
import cats.effect.IO
import cats.effect.std.Random
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.StorageClass
import software.amazon.awssdk.transfer.s3.internal.S3CrtAsyncClient

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.*

@IntegrationTest
class S3StoreIntegrationTest extends AbstractS3StoreTest {
  val bucketEnv = "S3_IT_BUCKET"
  val regionEnv = "S3_IT_REGION"
  def authority: Authority = sys.env.get(bucketEnv) match {
    case Some(bucket) => Authority.unsafe(bucket)
    case None         => cancel(show"No test bucket found. Please set $bucketEnv.")
  }

  def region: Region = sys.env.get(regionEnv)
    .flatMap(potentialRegion => Region.regions().asScala.find(_.id() == potentialRegion.toLowerCase)) match {
    case Some(r) => r
    case None    => cancel(show"No test bucket region found. Please set $regionEnv.")
  }

  lazy val client: S3AsyncClient = S3AsyncClient
    .builder()
    .region(region)
    .overrideConfiguration(overrideConfiguration)
    .httpClient(httpClient)
    .build()

  lazy val crtAsyncClient: S3CrtAsyncClient = S3CrtAsyncClient.builder().region(region).build()

  override def afterAll(): Unit = {
    store.remove(Url(scheme, authority, testRunRoot), recursive = true)
    super.afterAll()
  }

  override def mkStore(): Store[IO, S3Blob] = S3Store
    .builder[IO](client)
    .withCrtClient(crtAsyncClient)
    .withBufferSize(5 * 1024 * 1024)
    .enableFullMetadata
    .unsafe()

  it should "pick up correct storage class" in {
    val dir     = dirUrl("foo")
    val fileUrl = dir / "file"

    store.putContent(fileUrl, "test").unsafeRunSync()

    s3Store.listUnderlying(dir, fullMetadata = false, expectTrailingSlashFiles = false, recursive = true).map { u =>
      u.path.storageClass mustBe Some(StorageClass.STANDARD)
    }.compile.lastOrError.unsafeRunSync()

    val storeGeneric: Store.Generic[IO] = store

    storeGeneric.list(dir).map { u =>
      u.path.storageClass mustBe None
    }.compile.lastOrError.unsafeRunSync()
  }

  it should "handle files with trailing / in name" in {
    val dir      = dirUrl("trailing-slash")
    val filePath = dir / "file-with-slash/"

    store.putContent(filePath, "test").unsafeRunSync()

    val entities = s3Store
      .listUnderlying(dir, fullMetadata = false, expectTrailingSlashFiles = true, recursive = false)
      .compile
      .toList
      .unsafeRunSync()

    entities.foreach { listedUrl =>
      listedUrl.path.fileName mustBe Some("file-with-slash/")
      listedUrl.path.isDir mustBe false
    }

    store.getContents(filePath).unsafeRunSync() mustBe "test"

    store.remove(filePath).unsafeRunSync()

    store.list(dir).compile.toList.unsafeRunSync() mustBe Nil
  }

  it should "pick up metadata from resolved blob" in {
    val url = dirUrl("foo") / "bar" / "file"

    store.putContent(url, "test").unsafeRunSync()

    store.stat(url).map { url =>
      url.path.fullName mustBe show"$testRunRoot/foo/bar/file"
      url.path.size mustBe Some("test".getBytes(StandardCharsets.UTF_8).length)
      url.path.isDir mustBe false
      url.path.lastModified must not be None
      url.path.storageClass mustBe None // Not supported by minio
      url.path.dirName mustBe None
    }.compile.lastOrError.unsafeRunSync()
  }

  it should "set underlying metadata on write" in {
    val ct = "text/plain"
    val sc = StorageClass.REDUCED_REDUNDANCY
    val s3Meta =
      S3MetaInfo.const(constContentType = Some(ct), constStorageClass = Some(sc), constMetadata = Map("key" -> "Value"))

    val filePath = Path(show"test-$testRun/set-underlying/file1")
    Stream("data".getBytes.toIndexedSeq*).through(
      s3Store.put(Url("s3", authority, filePath), overwrite = true, size = None, meta = Some(s3Meta))
    ).compile.drain.unsafeRunSync()
    val entities = s3Store.list(Url("s3", authority, filePath)).compile.toList.unsafeRunSync()

    entities.foreach { s3Url =>
      inside(s3Url.path.representation.meta) {
        case Some(meta) =>
          meta.storageClass must contain(sc)
          meta.contentType must contain(ct)
          meta.metadata.map { case (k, v) => k.toLowerCase -> v.toLowerCase } mustBe Map("key" -> "value")
      }
    }
  }

  it should "set underlying metadata on multipart-upload" in {
    val ct = "text/plain"
    val sc = StorageClass.REDUCED_REDUNDANCY
    val s3Meta =
      S3MetaInfo.const(constContentType = Some(ct), constStorageClass = Some(sc), constMetadata = Map("Key" -> "Value"))
    val filePath = Path(show"test-$testRun/set-underlying/file2")
    Random.scalaUtilRandom[IO].flatMap(r =>
      Stream.repeatEval(r.nextInt)
        .flatMap(n => Stream.chunk(Chunk.ArraySlice(n.toString.getBytes())))
        .take(6 * 1024 * 1024)
        .through(s3Store.put(Url("s3", authority, filePath), overwrite = true, size = None, meta = Some(s3Meta)))
        .compile
        .drain
    )
      .unsafeRunSync()

    val entities = s3Store.list(Url("s3", authority, filePath)).compile.toList.unsafeRunSync()

    entities.foreach { s3Url =>
      inside(s3Url.path.representation.meta) {
        case Some(meta) =>
          meta.storageClass must contain(sc)
          meta.contentType must contain(ct)
          meta.metadata.map { case (k, v) => k.toLowerCase -> v.toLowerCase } mustBe Map("key" -> "value")
      }
    }
  }
}
