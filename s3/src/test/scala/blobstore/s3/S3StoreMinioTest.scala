package blobstore.s3

import cats.syntax.all._
import blobstore.url.{Path, Url}
import cats.effect.IO
import cats.effect.std.Random
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.GenericContainer
import fs2.{Chunk, Stream}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.StorageClass

import java.net.URI
import java.nio.charset.StandardCharsets

class S3StoreMinioTest extends AbstractS3StoreTest {
  override val container: GenericContainer = GenericContainer(
    dockerImage = "minio/minio",
    exposedPorts = List(9000),
    command = List("server", "--compat", "/data"),
    env = Map(
      "MINIO_ACCESS_KEY" -> "minio_access_key",
      "MINIO_SECRET_KEY" -> "minio_secret_key"
    )
  )

  override def client: S3AsyncClient = S3AsyncClient
    .builder()
    .region(Region.US_EAST_1)
    .endpointOverride(URI.create(s"http://${container.containerIpAddress}:${container.mappedPort(9000)}"))
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
      "minio_access_key",
      "minio_secret_key"
    )))
    .build()

  behavior of "S3 - MinIO test"

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

    val filePath = Path(s"test-$testRun/set-underlying/file1")
    Stream("data".getBytes.toIndexedSeq: _*).through(
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
    val filePath = Path(s"test-$testRun/set-underlying/file2")
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
