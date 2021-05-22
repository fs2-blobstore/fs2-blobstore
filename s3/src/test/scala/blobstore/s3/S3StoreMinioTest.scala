package blobstore.s3

import cats.effect.{IO, Resource}
import cats.syntax.all._
import fs2.Stream
import com.dimafeng.testcontainers.GenericContainer
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.StorageClass

import java.net.URI

object S3StoreMinioTest extends AbstractS3StoreTest {

  val container: GenericContainer = GenericContainer(
    dockerImage = "minio/minio",
    exposedPorts = List(9000),
    command = List("server", "--compat", "/data"),
    env = Map(
      "MINIO_ACCESS_KEY" -> "minio_access_key",
      "MINIO_SECRET_KEY" -> "minio_secret_key"
    )
  )

  def s3(host: String, port: Int): S3AsyncClient = S3AsyncClient
    .builder()
    .region(Region.US_EAST_1)
    .endpointOverride(URI.create(s"http://$host:$port"))
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
      "minio_access_key",
      "minio_secret_key"
    )))
    .build()

  override def clientResource: Resource[IO, S3AsyncClient] =
    Resource.make(IO.blocking(container.start()))(_ => IO.blocking(container.stop()))
      .map(_ => s3(container.containerIpAddress, container.mappedPort(9000)))

  test("set underlying metadata on write") { res =>
    val dir = dirUrl("set-underlying")
    val url = dir / "file.bin"

    val ct = "application/octet-stream"
    val sc = StorageClass.REDUCED_REDUNDANCY
    val s3Meta =
      S3MetaInfo.const(constContentType = Some(ct), constStorageClass = Some(sc), constMetadata = Map("key" -> "value"))

    val test = for {
      data <- randomBytes(25)
      _ <-
        Stream.emits(data).through(res.extra.put(url, overwrite = true, size = None, meta = Some(s3Meta))).compile.drain
      l <- res.store.listAll(url)
    } yield {
      l.map { u =>
        val meta = u.path.representation.meta
        expect.all(
          meta.flatMap(_.contentType).contains(ct),
          meta.flatMap(_.storageClass).contains(sc),
          meta.flatMap(_.metadata.get("key")).contains("value")
        )
      }.combineAll
    }

    test.timeout(res.timeout)
  }

  test("set underlying metadata on multipart-upload") { res =>
    val dir = dirUrl("set-underlying-multipart")
    val url = dir / "file.bin"

    val ct = "application/octet-stream"
    val sc = StorageClass.REDUCED_REDUNDANCY
    val s3Meta =
      S3MetaInfo.const(constContentType = Some(ct), constStorageClass = Some(sc), constMetadata = Map("key" -> "value"))
    val test = for {
      data <- randomBytes(6 * 1024 * 1024)
      _ <-
        Stream.emits(data).through(res.extra.put(url, overwrite = true, size = None, meta = Some(s3Meta))).compile.drain
      l <- res.store.listAll(url)
    } yield {
      l.map { u =>
        val meta = u.path.representation.meta
        expect.all(
          meta.flatMap(_.contentType).contains(ct),
          meta.flatMap(_.storageClass).contains(sc),
          meta.flatMap(_.metadata.get("key")).contains("value")
        )
      }.combineAll

    }

    test.timeout(res.timeout)
  }
}
