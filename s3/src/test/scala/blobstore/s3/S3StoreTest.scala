/*
Copyright 2018 LendUp Global, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package blobstore
package s3

import java.net.URI

import blobstore.url.{Authority, Path}
import blobstore.url.Authority.Bucket
import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.instances.list._
import cats.syntax.all._
import fs2.{Chunk, Stream}
import org.scalatest.Inside
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.auth.signer.AwsS3V4Signer
import software.amazon.awssdk.core.client.config.{ClientOverrideConfiguration, SdkAdvancedClientOption}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Configuration}
import software.amazon.awssdk.services.s3.model.{BucketAlreadyOwnedByYouException, CreateBucketRequest, StorageClass}

import scala.concurrent.ExecutionException

class S3StoreTest extends AbstractStoreTest[Authority.Bucket, S3Blob] with Inside {
  val credentials       = AwsBasicCredentials.create("my_access_key", "my_secret_key")
  val minioHost: String = Option(System.getenv("BLOBSTORE_MINIO_HOST")).getOrElse("minio-container")
  val minioPort: String = Option(System.getenv("BLOBSTORE_MINIO_PORT")).getOrElse("9000")
  private val client = S3AsyncClient
    .builder()
    .region(Region.US_EAST_1)
    .endpointOverride(URI.create(s"http://$minioHost:$minioPort"))
    .credentialsProvider(StaticCredentialsProvider.create(credentials))
    .serviceConfiguration(
      S3Configuration
        .builder()
        .pathStyleAccessEnabled(true)
        .build()
    )
    .overrideConfiguration(
      ClientOverrideConfiguration
        .builder()
        .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
        .build()
    )
    .build()

  override val store                       = new S3Store[IO](client, defaultFullMetadata = true, bufferSize = 5 * 1024 * 1024)
  override val authority: Authority.Bucket = Bucket.unsafe("blobstore-test-bucket")
  override val scheme: String              = "s3"
  override val fileSystemRoot: Path.Plain  = Path("")

  behavior of "S3Store"

  it should "expose underlying metadata" in {
    val dir  = dirUrl("expose-underlying")
    val path = writeFile(store, dir.path)("abc.txt")

    val entities = store.list(path).compile.toList.unsafeRunSync()

    entities.foreach { s3Path =>
      inside(s3Path.representation.meta) {
        case Some(metaInfo) =>
          // Note: defaultFullMetadata = true in S3Store constructor.
          metaInfo.contentType mustBe defined
          metaInfo.eTag mustBe defined
      }
    }
  }

  it should "set underlying metadata on write" in {
    val ct = "text/plain"
    val sc = StorageClass.REDUCED_REDUNDANCY
    val s3Meta =
      S3MetaInfo.const(constContentType = Some(ct), constStorageClass = Some(sc), constMetadata = Map("key" -> "Value"))

    val filePath = Path(s"test-$testRun/set-underlying/file1")
    Stream("data".getBytes.toIndexedSeq: _*).through(
      store.put(authority.s3 / filePath, true, None, Some(s3Meta))
    ).compile.drain.unsafeRunSync()
    val entities = store.list(authority.s3 / filePath).compile.toList.unsafeRunSync()

    entities.foreach { s3Path =>
      inside(s3Path.representation.meta) {
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
    Stream
      .random[IO]
      .flatMap(n => Stream.chunk(Chunk.bytes(n.toString.getBytes())))
      .take(6 * 1024 * 1024)
      .through(store.put(authority.s3 / filePath, overwrite = true, size = None, meta = Some(s3Meta)))
      .compile
      .drain
      .unsafeRunSync()

    val entities = store.list(authority.s3 / filePath).compile.toList.unsafeRunSync()

    entities.foreach { s3Path =>
      inside(s3Path.representation.meta) {
        case Some(meta) =>
          meta.storageClass must contain(sc)
          meta.contentType must contain(ct)
          meta.metadata.map { case (k, v) => k.toLowerCase -> v.toLowerCase } mustBe Map("key" -> "value")
      }
    }
  }

  def testUploadNoSize(size: Long, name: String) =
    for {
      bytes <- Stream
        .random[IO]
        .flatMap(n => Stream.chunk(Chunk.bytes(n.toString.getBytes())))
        .take(size)
        .compile
        .to(Array)
      path = Path(s"$authority/test-$testRun/multipart-upload/") / name
      url  = authority.s3 / path
      _         <- Stream.chunk(Chunk.bytes(bytes)).through(store.put(url, true, None)).compile.drain
      readBytes <- store.get(url, 4096).compile.to(Array)
      _         <- store.remove(url, false)
    } yield readBytes mustBe bytes

  it should "put content with no size when aligned with multi-upload boundaries 5mb" in {
    testUploadNoSize(5 * 1024 * 1024, "5mb").unsafeRunSync()
  }

  it should "put content with no size when aligned with multi-upload boundaries 15mb" in {
    testUploadNoSize(10 * 1024 * 1024, "10mb").unsafeRunSync()
  }

  it should "put content with no size when not aligned with multi-upload boundaries 7mb" in {
    testUploadNoSize(7 * 1024 * 1024, "7mb").unsafeRunSync()
  }

  it should "put content with no size when not aligned with multi-upload boundaries 12mb" in {
    testUploadNoSize(12 * 1024 * 1024, "12mb").unsafeRunSync()
  }

  it should "put rotating with file-limit > bufferSize" in {
    val dir     = dirUrl("put-rotating-s3")
    val content = randomBA(7 * 1024 * 1024)
    val data    = Stream.emits(content)

    val test = for {
      counter <- Ref.of[IO, Int](0)
      _ <- data
        .through(store.putRotate(counter.getAndUpdate(_ + 1).map(i => dir / s"$i"), 6 * 1024 * 1024))
        .compile
        .drain
      files        <- store.list(dir, recursive = true).compile.toList
      fileContents <- files.traverse(p => store.get(authority.s3 / p, 1024).compile.to(Array))
    } yield {
      files must have size 2
      files.flatMap(_.size) must contain theSameElementsAs List(6 * 1024 * 1024L, 1024 * 1024L)
      fileContents.flatten mustBe content.toList
    }

    test.unsafeRunSync()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    try {
      client.createBucket(CreateBucketRequest.builder().bucket(authority.show).build()).get()
    } catch {
      case e: ExecutionException if e.getCause.isInstanceOf[BucketAlreadyOwnedByYouException] =>
      // noop
    }
    ()
  }
}
