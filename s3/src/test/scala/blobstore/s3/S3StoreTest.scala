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

//import java.time.{LocalDate, ZoneOffset}
//import java.util.Date

import cats.effect.IO
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.ObjectMetadata
import fs2.Stream
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.scalatest.{Assertion, Inside}

class S3StoreTest extends AbstractStoreTest with Inside {

  val credentials         = new BasicAWSCredentials("my_access_key", "my_secret_key")
  val clientConfiguration = new ClientConfiguration()
  clientConfiguration.setSignerOverride("AWSS3V4SignerType")
  val minioHost: String = Option(System.getenv("BLOBSTORE_MINIO_HOST")).getOrElse("minio-container")
  val minioPort: String = Option(System.getenv("BLOBSTORE_MINIO_PORT")).getOrElse("9000")
  private val client: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withEndpointConfiguration(
      new AwsClientBuilder.EndpointConfiguration(s"http://$minioHost:$minioPort", Regions.US_EAST_1.name())
    )
    .withPathStyleAccessEnabled(true)
    .withClientConfiguration(clientConfiguration)
    .withCredentials(new AWSStaticCredentialsProvider(credentials))
    .build()
  private val transferManager: TransferManager = TransferManagerBuilder
    .standard()
    .withS3Client(client)
    .build()

  override val store: Store[IO] = new S3Store[IO](transferManager, blocker = blocker, defaultFullMetadata = true)
  override val root: String     = "blobstore-test-bucket"

  behavior of "S3Store"

  it should "expose underlying metadata" in {
    val dir  = dirPath("expose-underlying")
    val path = writeFile(store, dir)("abc.txt")

    val entities = store.list(path).map(S3Path.narrow).unNone.compile.toList.unsafeRunSync()

    entities.foreach { s3Path =>
      inside(s3Path.s3Entity) {
        case Right(file) =>
          // Note: defaultFullMetadata = true in S3Store constructor.
          file.metadata mustBe defined

          Option(file.summary.getETag) mustBe defined

          // Note: S3 doesn't set StorageClass for S3 Standard storage class objects, but Minio does.
          Option(file.summary.getStorageClass) mustBe defined
      }
    }
  }

  it should "set underlying metadata on write" in {
    val ct = "text/plain"
    val sc = "REDUCED_REDUNDANCY"
    val s3Metadata = {
      val meta = new ObjectMetadata
      meta.setContentType(ct)
      meta.addUserMetadata("key", "value")
      meta
    }

    val s3Summary = S3Path.S3File.summaryFromMetadata(root, s"test-$testRun/set-underlying/file", s3Metadata)
    s3Summary.setStorageClass(sc)

    val filePath = S3Path(Right(S3Path.S3File(s3Summary, Some(s3Metadata))))
    Stream("data".getBytes.toIndexedSeq: _*).through(store.put(filePath)).compile.drain.unsafeRunSync()
    val entities = store.list(filePath).map(S3Path.narrow).unNone.compile.toList.unsafeRunSync()

    entities.foreach { s3Path =>
      inside(s3Path.s3Entity) {
        case Right(file) =>
          file.summary.getStorageClass mustBe sc
          inside(file.metadata) {
            case Some(metadata) =>
              metadata.getContentType mustBe ct
              metadata.getUserMetaDataOf("key") mustBe "value"
          }
      }
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
      client.createBucket(root)
    } catch {
      case e: com.amazonaws.services.s3.model.AmazonS3Exception if e.getMessage.contains("BucketAlreadyOwnedByYou") =>
      // noop
    }
    ()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    try {
      client.shutdown()
    } catch {
      case _: Throwable =>
    }
  }
}
