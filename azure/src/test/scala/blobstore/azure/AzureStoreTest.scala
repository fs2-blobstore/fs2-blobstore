package blobstore
package azure

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import blobstore.url.Authority.Bucket
import blobstore.url.Path.Plain
import blobstore.url.{Path, Url}
import cats.effect.IO
import fs2.Stream
import com.azure.storage.blob.{BlobServiceAsyncClient, BlobServiceClientBuilder}
import com.azure.storage.blob.models.{AccessTier, BlobItemProperties, BlobType}
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import org.scalatest.Inside
import reactor.core.publisher.Mono

class AzureStoreTest extends AbstractStoreTest[Bucket, AzureBlob] with Inside {
  override val scheme: String        = "https"
  override val authority: Bucket     = Bucket.unsafe("container")
  override val fileSystemRoot: Plain = Path("")

  val options = new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, 2, 2, null, null, null) // scalafix:ok

  // TODO: Remove this once version of azure-storage-blob containing https://github.com/Azure/azure-sdk-for-java/pull/9123 is released
  val azuriteContainerIp: String = InetAddress.getByName("azurite-container").getHostAddress

  val azure: BlobServiceAsyncClient = new BlobServiceClientBuilder()
    .connectionString(
      s"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://$azuriteContainerIp:10000/devstoreaccount1;"
    )
    .retryOptions(options)
    .buildAsyncClient()

  val azureStore: AzureStore[IO] = new AzureStore(azure, defaultFullMetadata = true, defaultTrailingSlashFiles = true)

  override val store: Store[IO, Bucket, AzureBlob] =
    azureStore

  behavior of "AzureStore"

  it should "handle files with trailing / in name" in {
    val dir      = dirUrl("trailing-slash")
    val filePath = dir / "file-with-slash/"

    store.put("test", filePath).compile.drain.unsafeRunSync()

    val entities = store.list(dir).compile.toList.unsafeRunSync()
    entities.foreach { listedPath =>
      listedPath.fileName mustBe Some("file-with-slash/")
      listedPath.isDir mustBe false
    }

    store.getContents(filePath).unsafeRunSync() mustBe "test"

    store.remove(filePath).unsafeRunSync()

    store.list(dir).compile.toList.unsafeRunSync() mustBe Nil
  }

  it should "expose underlying metadata" in {
    val dir  = dirUrl("expose-underlying")
    val path = writeFile(store, dir.path)("abc.txt")

    val entities = store.list(path).compile.toList.unsafeRunSync()

    entities.foreach { azurePath =>
      inside(azurePath.representation.properties) {
        case Some(properties) =>
          Option(properties.getAccessTier) must contain(AccessTier.HOT)
          Option(properties.getBlobType) must contain(BlobType.BLOCK_BLOB)
      }
    }
  }

  it should "set underlying metadata on write" in {
    val ct                    = "text/plain"
    val at                    = AccessTier.COOL
    val properties            = new BlobItemProperties().setAccessTier(at).setContentType(ct)
    val filePath: Url[Bucket] = dirUrl("set-underlying") / "file"
//      new AzurePath(authority, s"test-$testRun/set-underlying/file", Some(properties), )
    Stream("data".getBytes.toIndexedSeq: _*).through(azureStore.put(
      filePath,
      overwrite = true,
      properties = Some(properties),
      meta = Map("key" -> "value")
    )).compile.drain.unsafeRunSync()

    val entities = store.list(filePath).compile.toList.unsafeRunSync()

    entities.foreach { azurePath =>
      azurePath.representation.metadata must contain key "key"
      azurePath.representation.metadata.get("key") must contain("value")
      inside(azurePath.representation.properties) {
        case Some(properties) =>
          Option(properties.getAccessTier) must contain(at)
          Option(properties.getContentType) must contain(ct)
      }
    }
  }

  it should "resolve type of storage class" in {
    store.list(dirUrl("foo")).map { path =>
      val sc: Option[AccessTier] = path.storageClass
      sc mustBe None
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val delete: Mono[Void] = azure
      .deleteBlobContainer(authority.host.toString)
      .onErrorResume(_ => Mono.empty())
    val create: Mono[Void]   = azure.createBlobContainer(authority.host.toString).flatMap(_ => Mono.empty())
    def recreate: Mono[Void] = delete.`then`(create).onErrorResume(_ => recreate)
    recreate.toFuture.get(1, TimeUnit.MINUTES)
    ()
  }

}
