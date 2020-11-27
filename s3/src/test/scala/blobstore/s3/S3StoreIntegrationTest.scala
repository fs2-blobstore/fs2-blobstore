package blobstore
package s3

import cats.effect.{ContextShift, IO}
import cats.effect.laws.util.TestInstances
import org.scalatest.{BeforeAndAfterAll, Inside}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import implicits._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.auth.signer.AwsS3V4Signer
import software.amazon.awssdk.core.client.config.{ClientOverrideConfiguration, SdkAdvancedClientOption}
import software.amazon.awssdk.services.s3.model.{
  Delete,
  DeleteObjectsRequest,
  ListObjectsV2Request,
  ObjectIdentifier,
  S3Object
}
import software.amazon.awssdk.services.s3.{S3AsyncClient, S3Configuration}

import scala.concurrent.ExecutionContext

@IntegrationTest
class S3StoreIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with TestInstances with Inside {

  // Your S3 access key. You are free to use a different way of instantiating your AmazonS3,
  // but be careful not to commit this information.
  lazy val s3AccessKey: String = sys.env("S3_TEST_ACCESS_KEY") // scalafix:ok

  // Your S3 secret key. You are free to use a different way of instantiating your AmazonS3,
  // but be careful not to commit this information.
  lazy val s3SecretKey: String = sys.env("S3_TEST_SECRET_KEY") // scalafix:ok

  lazy val s3Bucket: String = sys.env("S3_TEST_BUCKET") // scalafix:ok

  val testRun = java.util.UUID.randomUUID

  private lazy val credentials = AwsBasicCredentials.create(s3AccessKey, s3SecretKey)
  private lazy val client = S3AsyncClient
    .builder()
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

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  lazy val store: S3Store[IO] = new S3Store[IO](client, bufferSize = 5 * 1024 * 1024)

  behavior of "S3Store"

  // Keys with trailing slashes are perfectly legal in S3.
  // Note: Minio doesn't support keys with trailing slashes.
  it should "handle files with trailing / in name" in {
    val dir: Path = Path(s"$s3Bucket/test-$testRun/trailing-slash/").withIsDir(Some(true), reset = false)
    val filePath  = dir / "file-with-slash/"

    store.put("test", filePath).unsafeRunSync()

    val entities = store
      .listUnderlying(dir, fullMetadata = false, expectTrailingSlashFiles = true, recursive = false)
      .compile
      .toList
      .unsafeRunSync()
    entities.foreach { listedPath =>
      listedPath.fileName mustBe Some("file-with-slash/")
      listedPath.isDir mustBe Some(false)
    }

    store.getContents(filePath).unsafeRunSync() mustBe "test"

    store.remove(filePath).unsafeRunSync()

    store.list(dir).compile.toList.unsafeRunSync() mustBe empty
  }

  override protected def afterAll(): Unit = {
    try {
      val elements = new java.util.ArrayList[ObjectIdentifier]
      client
        .listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(s3Bucket).build())
        .contents()
        .subscribe { (t: S3Object) =>
          elements.add(ObjectIdentifier.builder().key(t.key()).build())
          ()
        }
        .get()
      client
        .deleteObjects(
          DeleteObjectsRequest.builder().bucket(s3Bucket).delete(Delete.builder().objects(elements).build()).build()
        )
        .get()
    } catch {
      case _: Throwable =>
    }
    super.afterAll()
  }
}
