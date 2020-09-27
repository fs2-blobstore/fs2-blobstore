//package blobstore
//package blobstore
//package azure
//
//import java.net.InetAddress
//import java.util.concurrent.TimeUnit
//
//import implicits._
//import cats.effect.IO
//import fs2.Stream
//import com.azure.storage.blob.BlobServiceClientBuilder
//import com.azure.storage.blob.models.{AccessTier, BlobItemProperties, BlobType}
//import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
//import org.scalatest.{Assertion, Inside}
//import reactor.core.publisher.Mono
//
//class AzureStoreTest extends AbstractStoreTest with Inside {
//  override val authority: String = "container"
//  val options               = new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, 2, 2, null, null, null)
//  // TODO: Remove this once version of azure-storage-blob containing https://github.com/Azure/azure-sdk-for-java/pull/9123 is released
//  val azuriteContainerIp = InetAddress.getByName("azurite-container").getHostAddress
//  val azure = new BlobServiceClientBuilder()
//    .connectionString(
//      s"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://$azuriteContainerIp:10000/devstoreaccount1;"
//    )
//    .retryOptions(options)
//    .buildAsyncClient()
//
//  override val store: Store[IO] = new AzureStore(azure, defaultFullMetadata = true, defaultTrailingSlashFiles = true)
//
//  behavior of "AzureStore"
//
//  it should "handle files with trailing / in name" in {
//    val dir: Path = dirPath("trailing-slash")
//    val filePath  = dir / "file-with-slash/"
//
//    store.put("test", filePath).unsafeRunSync()
//
//    // Note
//    val entities = store.list(dir).compile.toList.unsafeRunSync()
//    entities.foreach { listedPath =>
//      listedPath.fileName mustBe Some("file-with-slash/")
//      listedPath.isDir mustBe Some(false)
//    }
//
//    store.getContents(filePath).unsafeRunSync() mustBe "test"
//
//    store.remove(filePath).unsafeRunSync()
//
//    store.list(dir).compile.toList.unsafeRunSync() mustBe empty
//  }
//
//  it should "expose underlying metadata" in {
//    val dir  = dirPath("expose-underlying")
//    val path = writeLocalFile(store, dir)("abc.txt")
//
//    val entities = store.list(path).map(AzurePath.narrow).unNone.compile.toList.unsafeRunSync()
//
//    entities.foreach { azurePath =>
//      azurePath.properties mustBe defined
//      inside(azurePath.properties) {
//        case Some(properties) =>
//          Option(properties.getAccessTier) must contain(AccessTier.HOT)
//          Option(properties.getBlobType) must contain(BlobType.BLOCK_BLOB)
//      }
//    }
//  }
//
//  it should "set underlying metadata on write" in {
//    val ct         = "text/plain"
//    val at         = AccessTier.COOL
//    val properties = new BlobItemProperties().setAccessTier(at).setContentType(ct)
//    val filePath   = new AzurePath(authority, s"test-$testRun/set-underlying/file", Some(properties), Map("key" -> "value"))
//    Stream("data".getBytes.toIndexedSeq: _*).through(store.put(filePath)).compile.drain.unsafeRunSync()
//    val entities = store.list(filePath).map(AzurePath.narrow).unNone.compile.toList.unsafeRunSync()
//
//    entities.foreach { azurePath =>
//      azurePath.meta must contain key "key"
//      azurePath.meta.get("key") must contain("value")
//      inside(azurePath.properties) {
//        case Some(properties) =>
//          Option(properties.getAccessTier) must contain(at)
//          Option(properties.getContentType) must contain(ct)
//      }
//    }
//  }
//
//  it should "fail trying to reference path with no root" in {
//    val path = Path("s3://bucket/folder/file")
//    inside[Option[Path], Assertion](Path.fromString("/folder/file.txt", forceRoot = false)) {
//      case Some(pathNoRoot) =>
//        the[IllegalArgumentException] thrownBy store
//          .get(pathNoRoot, 10)
//          .compile
//          .drain
//          .unsafeRunSync() must have message s"Unable to read '$pathNoRoot' - root (container) is required to reference blobs in Azure Storage"
//        the[IllegalArgumentException] thrownBy Stream.empty
//          .through(store.put(pathNoRoot))
//          .compile
//          .drain
//          .unsafeRunSync() must have message s"Unable to write to '$pathNoRoot' - root (container) is required to reference blobs in Azure Storage"
//        the[IllegalArgumentException] thrownBy store
//          .list(pathNoRoot)
//          .compile
//          .drain
//          .unsafeRunSync() must have message s"Unable to list '$pathNoRoot' - root (container) is required to reference blobs in Azure Storage"
//        the[IllegalArgumentException] thrownBy store
//          .remove(pathNoRoot)
//          .unsafeRunSync() must have message s"Unable to remove '$pathNoRoot' - root (container) is required to reference blobs in Azure Storage"
//        the[IllegalArgumentException] thrownBy store
//          .copy(pathNoRoot, path)
//          .unsafeRunSync() must have message s"Wrong src '$pathNoRoot' - root (container) is required to reference blobs in Azure Storage"
//        the[IllegalArgumentException] thrownBy store
//          .copy(path, pathNoRoot)
//          .unsafeRunSync() must have message s"Wrong dst '$pathNoRoot' - root (container) is required to reference blobs in Azure Storage"
//    }
//  }
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//    azure
//      .deleteBlobContainer(authority)
//      .onErrorResume(_ => Mono.empty())
//      .`then`(azure.createBlobContainer(authority))
//      .toFuture
//      .get(1, TimeUnit.MINUTES)
//    ()
//  }
//}
