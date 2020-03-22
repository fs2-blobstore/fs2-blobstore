package blobstore
package s3

import java.util.concurrent.Executors

import cats.effect.{Blocker, ContextShift, IO}
import cats.effect.laws.util.TestInstances
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import org.scalatest.{BeforeAndAfterAll, Inside}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import implicits._

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext

@IntegrationTest
class S3StoreIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with TestInstances with Inside {

  // Your S3 access key. You are free to use a different way of instantiating your AmazonS3,
  // but be careful not to commit this information.
  val s3AccessKey: String = System.getenv("S3_TEST_ACCESS_KEY")

  // Your S3 secret key. You are free to use a different way of instantiating your AmazonS3,
  // but be careful not to commit this information.
  val s3SecretKey: String = System.getenv("S3_TEST_SECRET_KEY")

  // Your S3 bucket name.

  val s3Bucket: String = System.getenv("S3_TEST_BUCKET")

  val testRun = java.util.UUID.randomUUID

  lazy val credentials    = new BasicAWSCredentials(s3AccessKey, s3SecretKey)
  val clientConfiguration = new ClientConfiguration()
  clientConfiguration.setSignerOverride("AWSS3V4SignerType")
  private lazy val client: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withPathStyleAccessEnabled(true)
    .withClientConfiguration(clientConfiguration)
    .withCredentials(new AWSStaticCredentialsProvider(credentials))
    .build()
  private lazy val transferManager: TransferManager = TransferManagerBuilder
    .standard()
    .withS3Client(client)
    .build()

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private val blocker                       = Blocker.liftExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))

  lazy val store: S3Store[IO] = new S3Store[IO](transferManager, blocker = blocker)

  behavior of "S3Store"

  // Keys with trailing slashes are perfectly legal in S3.
  // Note: Minio doesn't support keys with trailing slashes.
  it should "handle files with trailing / in name" in {
    val dir: Path = Path(s"$s3Bucket/test-$testRun/trailing-slash/").withIsDir(Some(true), reset = false)
    val filePath  = dir / "file-with-slash/"

    store.put("test", filePath).unsafeRunSync()

    val entities = store
      .listUnderlying(dir, fullMetadata = false, expectTrailingSlashFiles = true)
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

  override def beforeAll(): Unit = {
    super.beforeAll()
    try {
      client.createBucket(s3Bucket)
    } catch {
      case e: com.amazonaws.services.s3.model.AmazonS3Exception if e.getMessage.contains("BucketAlreadyOwnedByYou") =>
      // noop
    }
    ()
  }

  override protected def afterAll(): Unit = {
    try {
      val req = new DeleteObjectsRequest(s3Bucket)
        .withKeys(client.listObjects(s3Bucket).getObjectSummaries.asScala.map(_.getKey).toIndexedSeq: _*)
        .withQuiet(true)
      client.deleteObjects(req)
      client.deleteBucket(s3Bucket)
      client.shutdown()
    } catch {
      case _: Throwable =>
    }
    super.afterAll()
  }
}
