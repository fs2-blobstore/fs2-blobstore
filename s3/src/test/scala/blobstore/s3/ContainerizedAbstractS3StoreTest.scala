package blobstore.s3

import blobstore.url.Authority
import cats.effect.IO
import cats.syntax.all.*
import com.dimafeng.testcontainers.GenericContainer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.CreateBucketRequest

abstract class ContainerizedAbstractS3StoreTest extends AbstractS3StoreTest {
  def container: GenericContainer
  def client: S3AsyncClient
  override val authority: Authority = Authority.unsafe("blobstore-test-bucket")
  override def mkStore(): S3Store[IO] = S3Store
    .builder[IO](client)
    .withBufferSize(5 * 1024 * 1024)
    .enableFullMetadata
    .unsafe

  override def beforeAll(): Unit = {
    container.start()

    val request: CreateBucketRequest = CreateBucketRequest.builder().bucket(authority.show).build()
    println(request.toString)
    client.createBucket(request).get()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    container.stop()
    super.afterAll()
  }
}
