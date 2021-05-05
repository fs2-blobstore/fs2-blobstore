package blobstore
package azure

import blobstore.url.{Authority, Path}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import fs2.Stream
import com.azure.storage.blob.{BlobServiceAsyncClient, BlobServiceClientBuilder}
import com.azure.storage.blob.models.{AccessTier, BlobItemProperties, BlobType}
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import com.dimafeng.testcontainers.GenericContainer
import reactor.core.publisher.{Hooks, Operators}
import reactor.util.{Logger, Loggers}
import weaver.GlobalRead

import java.util.function.Consumer

class AzureStoreTest(global: GlobalRead) extends AbstractStoreTest[AzureBlob, AzureStore[IO]](global) {

  override def maxParallelism = 1

  // TODO: Fix underlying issue and remove this
  val logger: Logger = Loggers.getLogger(classOf[Operators])
  Hooks.onErrorDropped(new Consumer[Throwable] {
    // Hide those verbose "Operator called default onErrorDropped" logs on
    // java.lang.IllegalStateException: dispatcher already shutdown
    override def accept(t: Throwable): Unit = logger.debug("Exception happened:", t)
  })

  val container: GenericContainer = GenericContainer(
    dockerImage = "mcr.microsoft.com/azure-storage/azurite",
    exposedPorts = List(10000),
    command = List("azurite-blob", "--blobHost", "0.0.0.0", "--loose")
  )

  override val scheme: String       = "https"
  override val authority: Authority = Authority.unsafe("container")

  override val fileSystemRoot: Path.Plain = Path("")
  override val testRunRoot: Path.Plain    = Path(testRun.toString)

  def azure(host: String, port: Int): BlobServiceAsyncClient =
    new BlobServiceClientBuilder()
      .connectionString(
        s"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://$host:$port/devstoreaccount1;"
      )
      .retryOptions(
        new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, 2, 2, null, null, null) // scalafix:ok
      )
      .buildAsyncClient()

  def blobContainer(a: BlobServiceAsyncClient): Resource[IO, Unit] = Resource.make {
    IO.fromCompletableFuture(IO(a.createBlobContainer(authority.host.toString).toFuture))
  } { bcClient =>
    IO.fromCompletableFuture(IO(bcClient.delete().toFuture)).void
  }.void

  override val sharedResource: Resource[IO, TestResource[AzureBlob, AzureStore[IO]]] =
    Resource.make(IO.blocking(container.start()))(_ => IO.blocking(container.stop()))
      .map(_ => azure(container.containerIpAddress, container.mappedPort(10000)))
      .flatMap(a => blobContainer(a).as(a))
      .map { a =>
        val azureStore = new AzureStore(a, defaultFullMetadata = true, defaultTrailingSlashFiles = true)
        TestResource(azureStore, azureStore)
      }

  test("handle files with trailing / in name") { res =>
    val dir = dirUrl("trailing-slash")
    val url = dir / "file-with-slash/"
    for {
      data    <- randomBytes(25)
      _       <- Stream.emits(data).through(res.store.put(url)).compile.drain
      l1      <- res.store.listAll(dir)
      content <- res.store.get(url, 128).compile.to(Array)
      _       <- res.store.remove(url)
      l2      <- res.store.listAll(dir)
    } yield {
      expect.all(
        data sameElements content,
        l1.size == 1,
        l2.isEmpty
      ) and l1.map(url =>
        expect.all(
          url.path.fileName.contains("file-with-slash/"),
          !url.path.isDir
        )
      ).combineAll
    }

  }

  test("expose underlying metadata") { (res, log) =>
    val dir = dirUrl("expose-underlying")
    for {
      (url, _) <- writeRandomFile(res.store, log)(dir)
      l        <- res.store.listAll(url)
    } yield {
      l.map { u =>
        expect.all(
          u.path.representation.properties.map(_.getAccessTier).contains(AccessTier.HOT),
          u.path.representation.properties.map(_.getBlobType).contains(BlobType.BLOCK_BLOB)
        )
      }.combineAll
    }
  }

  test("set underlying metadata on write") { res =>
    val dir = dirUrl("set-underlying")
    val url = dir / "file.bin"

    val ct         = "application/octet-stream"
    val at         = AccessTier.COOL
    val properties = new BlobItemProperties().setAccessTier(at).setContentType(ct)

    for {
      data <- randomBytes(25)
      _ <- Stream.emits(data).through(
        res.extra.put(url, overwrite = true, properties = Some(properties), meta = Map("key" -> "value"))
      ).compile.drain
      l <- res.store.listAll(url)
    } yield {
      l.map { u =>
        expect.all(
          u.path.representation.properties.map(_.getContentType).contains(ct),
          u.path.representation.properties.map(_.getAccessTier).contains(at),
          u.path.representation.metadata.get("key").contains("value")
        )
      }.combineAll

    }
  }

  test("resolve type of storage class") { res =>
    res.store.listAll(dirUrl("storage-class")).map { l =>
      l.map { u =>
        val sc: Option[AccessTier] = u.path.storageClass
        expect(sc.isEmpty)
      }.combineAll
    }
  }

}
