package blobstore
package s3

import cats.effect.{IO, Resource}
import fs2.Stream
import cats.syntax.all._
import java.net.URI
import com.dimafeng.testcontainers.GenericContainer
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import weaver.GlobalRead

class S3StoreS3MockTest(global: GlobalRead) extends AbstractS3StoreTest(global) {

  override def maxParallelism = 1

  val container: GenericContainer = GenericContainer(
    dockerImage = "adobe/s3mock",
    exposedPorts = List(9090)
  )

  def s3(host: String, port: Int): S3AsyncClient =
    S3AsyncClient
      .builder()
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create(s"http://$host:$port"))
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("a", "s")))
      .build()

  override def clientResource: Resource[IO, S3AsyncClient] =
    Resource.make(IO.blocking(container.start()))(_ => IO.blocking(container.stop()))
      .map(_ => s3(container.containerIpAddress, container.mappedPort(9090)))

  test("handle files with trailing / in name") { res =>
    val dir = dirUrl("trailing-slash")
    val url = dir / "file-with-slash/"
    val test = for {
      data <- randomBytes(25)
      _    <- Stream.emits(data).through(res.store.put(url)).compile.drain
      l1 <- res.extra.listUnderlying(
        dir,
        fullMetadata = false,
        expectTrailingSlashFiles = true,
        recursive = false
      ).compile.toList
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

    test.timeout(res.timeout)

  }

}
