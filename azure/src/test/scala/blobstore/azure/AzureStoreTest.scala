package blobstore
package azure

import java.util.concurrent.TimeUnit
import blobstore.url.Path.Plain
import blobstore.url.{Authority, Path}
import blobstore.url.general.GeneralStorageClass
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import com.azure.storage.blob.{BlobServiceAsyncClient, BlobServiceClientBuilder}
import com.azure.storage.blob.models.{AccessTier, BlobItemProperties, BlobType}
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import com.dimafeng.testcontainers.GenericContainer
import org.scalatest.Inside

class AzureStoreTest extends AbstractStoreTest[AzureBlob] with Inside {

  val container: GenericContainer = GenericContainer(
    dockerImage = "mcr.microsoft.com/azure-storage/azurite",
    exposedPorts = List(10000),
    command = List("azurite-blob", "--blobHost", "0.0.0.0", "--loose")
  )

  override val scheme: String        = "https"
  override val authority: Authority  = Authority.unsafe("container")
  override val fileSystemRoot: Plain = Path("")

  val options = new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, 2, 2, null, null, null) // scalafix:ok

  lazy val azure: BlobServiceAsyncClient = new BlobServiceClientBuilder()
    .connectionString(
      s"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://${container.containerIpAddress}:${container.mappedPort(10000)}/devstoreaccount1;"
    )
    .retryOptions(options)
    .buildAsyncClient()

  override def mkStore(): Store[IO, AzureBlob] =
    new AzureStore(azure, defaultFullMetadata = true, defaultTrailingSlashFiles = true)

  def azureStore: AzureStore[IO] = store.asInstanceOf[AzureStore[IO]] // scalafix:ok

  override def beforeAll(): Unit = {
    container.start()
    azure.createBlobContainer(authority.host.toString).toFuture.get(1, TimeUnit.MINUTES)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    container.stop()
    super.afterAll()
  }

  behavior of "AzureStore"

  it should "pick up correct storage class" in {
    val dir     = dirUrl("storage-class")
    val fileUrl = dir / "file"

    store.putContent(fileUrl, "test").unsafeRunSync()

    store.list(fileUrl).map { u =>
      u.path.storageClass mustBe Some(AccessTier.HOT)
    }.compile.lastOrError.unsafeRunSync()

    val storeGeneric: Store.Generic[IO] = store

    storeGeneric.list(fileUrl).map { u =>
      u.path.storageClass mustBe Some(GeneralStorageClass.Standard)
    }.compile.lastOrError.unsafeRunSync()
  }

  it should "handle files with trailing / in name" in {
    val dir      = dirUrl("trailing-slash")
    val filePath = dir / "file-with-slash/"

    store.putContent(filePath, "test").unsafeRunSync()

    val entities = store.list(dir).compile.toList.unsafeRunSync()
    entities.foreach { listedUrl =>
      listedUrl.path.fileName mustBe Some("file-with-slash/")
      listedUrl.path.isDir mustBe false
    }

    store.getContents(filePath).unsafeRunSync() mustBe "test"

    store.remove(filePath).unsafeRunSync()

    store.list(dir).compile.toList.unsafeRunSync() mustBe Nil
  }

  it should "expose underlying metadata" in {
    val dir  = dirUrl("expose-underlying")
    val path = writeFile(store, dir)("abc.txt")

    val entities = store.list(path).compile.toList.unsafeRunSync()

    entities.foreach { azureUrl =>
      inside(azureUrl.path.representation.properties) {
        case Some(properties) =>
          Option(properties.getAccessTier) must contain(AccessTier.HOT)
          Option(properties.getBlobType) must contain(BlobType.BLOCK_BLOB)
      }
    }
  }

  it should "set underlying metadata on write" in {
    val ct         = "text/plain"
    val at         = AccessTier.COOL
    val properties = new BlobItemProperties().setAccessTier(at).setContentType(ct)
    val fileUrl    = dirUrl("set-underlying") / "file"
    Stream("data".getBytes.toIndexedSeq: _*).through(azureStore.put(
      fileUrl,
      overwrite = true,
      properties = Some(properties),
      meta = Map("key" -> "value")
    )).compile.drain.unsafeRunSync()

    val entities = store.list(fileUrl).compile.toList.unsafeRunSync()

    entities.foreach { azureUrl =>
      azureUrl.path.representation.metadata must contain key "key"
      azureUrl.path.representation.metadata.get("key") must contain("value")
      inside(azureUrl.path.representation.properties) {
        case Some(properties) =>
          Option(properties.getAccessTier) must contain(at)
          Option(properties.getContentType) must contain(ct)
      }
    }
  }

  it should "resolve type of storage class" in {
    store.list(dirUrl("foo")).map { url =>
      val sc: Option[AccessTier] = url.path.storageClass
      sc mustBe None
    }
  }

}
