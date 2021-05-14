package blobstore
package azure

import blobstore.url.{Authority, Path}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import fs2.Stream
import com.azure.storage.blob.models.{AccessTier, BlobItemProperties, BlobType}
import com.azure.storage.blob.{BlobServiceAsyncClient, BlobServiceClientBuilder}
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import com.dimafeng.testcontainers.GenericContainer
import reactor.core.publisher.Hooks
import weaver.GlobalRead

import java.util.function.Consumer
import scala.concurrent.duration.FiniteDuration

class AzureStoreTest(global: GlobalRead) extends AbstractStoreTest[AzureBlob, AzureStore[IO]](global) {

  Hooks.onErrorDropped(new Consumer[Throwable] {
    override def accept(t: Throwable): Unit = ()
  })

  val maybeConnectionString: IO[Option[String]] = IO(sys.env.get("AZURE_CONNECTION_STRING"))

  val maybeAzuritePort: IO[Option[Int]] =
    IO(sys.env.get("AZURITE_PORT")).flatMap(_.fold(IO.none[Int])(mp => IO(mp.toInt.some)))

  val container: GenericContainer = GenericContainer(
    dockerImage = "mcr.microsoft.com/azure-storage/azurite",
    exposedPorts = List(10000),
    command = List("azurite-blob", "--blobHost", "0.0.0.0", "--loose")
  )

  override val scheme: String       = "https"
  override val authority: Authority = Authority.unsafe(s"container-$testRun")

  override val fileSystemRoot: Path.Plain = Path("")
  override val testRunRoot: Path.Plain    = Path(testRun.toString)

  def azure(connectionString: String): BlobServiceAsyncClient =
    new BlobServiceClientBuilder()
      .connectionString(connectionString)
      .retryOptions(
        new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, 5, 5, null, null, null) // scalafix:ok
      )
      .buildAsyncClient()

  def blobContainer(a: BlobServiceAsyncClient): Resource[IO, Unit] = Resource.make {
    IO.fromCompletableFuture(IO(a.createBlobContainer(authority.host.toString).toFuture))
  } { bcClient =>
    IO.fromCompletableFuture(IO(bcClient.delete().toFuture)).void
  }.void

  val connectionString: Resource[IO, String] =
    Resource.eval(maybeConnectionString).flatMap {
      case Some(connectionString) => Resource.pure(connectionString)
      case None => for {
          maybePort <- Resource.eval(maybeAzuritePort)
          (host, port) <- maybePort match {
            case Some(p) => Resource.pure[IO, (String, Int)]("localhost" -> p)
            case None => Resource.make(IO.blocking(container.start()))(_ => IO.blocking(container.stop())).map(_ =>
                container.containerIpAddress -> container.mappedPort(10000)
              )
          }
        } yield s"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://$host:$port/devstoreaccount1;"
    }

  override val sharedResource: Resource[IO, TestResource[AzureBlob, AzureStore[IO]]] =
    connectionString.map(azure).flatMap(a => blobContainer(a).as(a)).map { a =>
      val azureStore = new AzureStore(a, defaultFullMetadata = true, defaultTrailingSlashFiles = true)
      TestResource(azureStore, azureStore, FiniteDuration(20, "s"))
    }

  test("handle files with trailing / in name") { res =>
    val dir = dirUrl("trailing-slash")
    val url = dir / "file-with-slash/"
    val test = for {
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

    test.timeout(res.timeout)
  }

  test("expose underlying metadata") { (res, log) =>
    val dir = dirUrl("expose-underlying")
    val test = for {
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

    test.timeout(res.timeout)
  }

  test("set underlying metadata on write") { res =>
    val dir = dirUrl("set-underlying")
    val url = dir / "file.bin"

    val ct         = "application/octet-stream"
    val at         = AccessTier.COOL
    val properties = new BlobItemProperties().setAccessTier(at).setContentType(ct)

    val test = for {
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

    test.timeout(res.timeout)
  }

  test("resolve type of storage class") { res =>
    val test = res.store.listAll(dirUrl("storage-class")).map { l =>
      l.map { u =>
        val sc: Option[AccessTier] = u.path.storageClass
        expect(sc.isEmpty)
      }.combineAll
    }

    test.timeout(res.timeout)
  }

}
