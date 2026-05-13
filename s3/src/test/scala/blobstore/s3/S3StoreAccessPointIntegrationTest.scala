package blobstore.s3

import blobstore.{IntegrationTest, Store}
import blobstore.url.{Authority, Url}
import cats.effect.IO
import cats.syntax.all.*
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Configuration}
import software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient

import scala.jdk.CollectionConverters.*

@IntegrationTest
class S3StoreAccessPointIntegrationTest extends AbstractS3StoreTest {
  val accessPointEnv = "S3_IT_ACCESS_POINT"
  val regionEnv      = "S3_IT_REGION"

  def authority: Authority = sys.env.get(accessPointEnv) match {
    case Some(ap) => Authority.unsafe(ap)
    case None     =>
      cancel(
        show"No access point found. Please set $accessPointEnv to an access-point alias or virtual-hosted-style hostname."
      )
  }

  def region: Region = sys.env.get(regionEnv)
    .flatMap(potentialRegion => Region.regions().asScala.find(_.id() == potentialRegion.toLowerCase)) match {
    case Some(r) => r
    case None    => cancel(show"No test region found. Please set $regionEnv.")
  }

  lazy val client: S3AsyncClient = S3AsyncClient
    .builder()
    .region(region)
    .serviceConfiguration(S3Configuration.builder().useArnRegionEnabled(true).build())
    .overrideConfiguration(overrideConfiguration)
    .httpClient(httpClient)
    .build()

  lazy val crtAsyncClient: S3AsyncClient = S3CrtAsyncClient.builder().region(region).build()

  override def afterAll(): Unit = {
    store.remove(Url(scheme, authority, testRunRoot), recursive = true)
    super.afterAll()
  }

  override def mkStore(): Store[IO, S3Blob] = S3Store
    .builder[IO](client)
    .withCrtClient(crtAsyncClient)
    .withBufferSize(5 * 1024 * 1024)
    .enableFullMetadata
    .unsafe
}
