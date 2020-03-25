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

import cats.effect.IO
import org.scalatest.{Assertion, Inside}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.auth.signer.AwsS3V4Signer
import software.amazon.awssdk.core.client.config.{ClientOverrideConfiguration, SdkAdvancedClientOption}
import software.amazon.awssdk.services.s3.model.{BucketAlreadyOwnedByYouException, CreateBucketRequest, StorageClass}
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Configuration}
import fs2.{Chunk, Stream}
import software.amazon.awssdk.regions.Region
import implicits._

import scala.concurrent.ExecutionException

class S3StoreTest extends AbstractStoreTest with Inside {
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

  override val store: Store[IO] = new S3Store[IO](client, defaultFullMetadata = true, bufferSize = 5 * 1024 * 1024)
  override val root: String     = "blobstore-test-bucket"

  behavior of "S3Store"

  it should "expose underlying metadata" in {
    val dir  = dirPath("expose-underlying")
    val path = writeFile(store, dir)("abc.txt")

    val entities = store.list(path).map(S3Path.narrow).unNone.compile.toList.unsafeRunSync()

    entities.foreach { s3Path =>
      inside(s3Path.meta) {
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
      S3MetaInfo.const(constContentType = Some(ct), constStorageClass = Some(sc), constMetadata = Map("Key" -> "Value"))

    val filePath = S3Path(root, s"test-$testRun/set-underlying/file1", Some(s3Meta))
    Stream("data".getBytes.toIndexedSeq: _*).through(store.put(filePath)).compile.drain.unsafeRunSync()
    val entities = store.list(filePath).map(S3Path.narrow).unNone.compile.toList.unsafeRunSync()

    entities.foreach { s3Path =>
      inside(s3Path.meta) {
        case Some(meta) =>
          meta.storageClass must contain(sc)
          meta.contentType must contain(ct)
          meta.metadata mustBe Map("Key" -> "Value")
      }
    }
  }

  it should "set underlying metadata on multipart-upload" in {
    val ct = "text/plain"
    val sc = StorageClass.REDUCED_REDUNDANCY
    val s3Meta =
      S3MetaInfo.const(constContentType = Some(ct), constStorageClass = Some(sc), constMetadata = Map("Key" -> "Value"))
    val filePath = S3Path(root, s"test-$testRun/set-underlying/file2", Some(s3Meta))
    Stream
      .random[IO]
      .flatMap(n => Stream.chunk(Chunk.bytes(n.toString.getBytes())))
      .take(6 * 1024 * 1024)
      .through(store.put(filePath))
      .compile
      .drain
      .unsafeRunSync()

    val entities = store.list(filePath).map(S3Path.narrow).unNone.compile.toList.unsafeRunSync()

    entities.foreach { s3Path =>
      inside(s3Path.meta) {
        case Some(meta) =>
          meta.storageClass must contain(sc)
          meta.contentType must contain(ct)
          meta.metadata mustBe Map("Key" -> "Value")
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
      path = Path(s"$root/test-$testRun/multipart-upload/").withIsDir(Some(true), reset = false) / name
      _         <- Stream.chunk(Chunk.bytes(bytes)).through(store.put(path)).compile.drain
      readBytes <- store.get(path).compile.to(Array)
      _         <- store.remove(path)
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

  it should "fail trying to reference path with no root" in {
    val path = Path("s3://bucket/folder/file")
    inside[Option[Path], Assertion](Path.fromString("/folder/file.txt", forceRoot = false)) {
      case Some(pathNoRoot) =>
        the[IllegalArgumentException] thrownBy store
          .get(pathNoRoot, 10)
          .compile
          .drain
          .unsafeRunSync() must have message s"Unable to read '$pathNoRoot' - root (bucket) is required to reference blobs in S3"
        the[IllegalArgumentException] thrownBy Stream.empty
          .through(store.put(pathNoRoot))
          .compile
          .drain
          .unsafeRunSync() must have message s"Unable to write to '$pathNoRoot' - root (bucket) is required to reference blobs in S3"
        the[IllegalArgumentException] thrownBy store
          .list(pathNoRoot)
          .compile
          .drain
          .unsafeRunSync() must have message s"Unable to list '$pathNoRoot' - root (bucket) is required to reference blobs in S3"
        the[IllegalArgumentException] thrownBy store
          .remove(pathNoRoot)
          .unsafeRunSync() must have message s"Unable to remove '$pathNoRoot' - root (bucket) is required to reference blobs in S3"
        the[IllegalArgumentException] thrownBy store
          .copy(pathNoRoot, path)
          .unsafeRunSync() must have message s"Wrong src '$pathNoRoot' - root (bucket) is required to reference blobs in S3"
        the[IllegalArgumentException] thrownBy store
          .copy(path, pathNoRoot)
          .unsafeRunSync() must have message s"Wrong dst '$pathNoRoot' - root (bucket) is required to reference blobs in S3"
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    try {
      client.createBucket(CreateBucketRequest.builder().bucket(root).build()).get()
    } catch {
      case e: ExecutionException if e.getCause.isInstanceOf[BucketAlreadyOwnedByYouException] =>
      // noop
    }
    ()
  }
}
