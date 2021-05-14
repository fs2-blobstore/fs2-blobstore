package blobstore.s3

import blobstore.{AbstractStoreTest, Store, TestResource}
import blobstore.url.{Authority, Path}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import fs2.Stream
import fs2.interop.reactivestreams._
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  CreateBucketRequest,
  Delete,
  DeleteBucketRequest,
  DeleteObjectsRequest,
  ListObjectsV2Request,
  ObjectIdentifier,
  StorageClass
}
import weaver.{Expectations, GlobalRead}

import scala.concurrent.duration.FiniteDuration

abstract class AbstractS3StoreTest(global: GlobalRead) extends AbstractStoreTest[S3Blob, S3Store[IO]](global) {
  override val scheme: String             = "s3"
  override val authority: Authority       = Authority.unsafe("blobstore-test-bucket")
  override val fileSystemRoot: Path.Plain = Path("")
  override val testRunRoot: Path.Plain    = Path(testRun.toString)

  def clientResource: Resource[IO, S3AsyncClient]

  def cleanup(client: S3AsyncClient): IO[Unit] = {
    val deleteObjects = client.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(authority.show).build())
      .contents().toStream
      .groupWithin(1000, FiniteDuration(1, "s"))
      .evalMap { chunk =>
        val objects = chunk.map(o => ObjectIdentifier.builder().key(o.key()).build()).toList
        val r = DeleteObjectsRequest
          .builder()
          .bucket(authority.show)
          .delete(Delete.builder().objects(objects: _*).build())
          .build()
        IO.fromCompletableFuture(IO(client.deleteObjects(r))).void
      }.compile.drain

    val deleteBucket = IO.fromCompletableFuture(
      IO(client.deleteBucket(DeleteBucketRequest.builder().bucket(authority.show).build()))
    ).void

    deleteObjects >> deleteBucket
  }

  def bucketResource(client: S3AsyncClient): Resource[IO, Unit] = Resource.make(
    IO.fromCompletableFuture(IO(client.createBucket(CreateBucketRequest.builder().bucket(authority.show).build()))).void
  )(_ => cleanup(client))

  override val sharedResource: Resource[IO, TestResource[S3Blob, S3Store[IO]]] = clientResource
    .flatMap(client => bucketResource(client).as(client))
    .map { client =>
      val s3Store = new S3Store[IO](client, defaultFullMetadata = true, bufferSize = 5 * 1024 * 1024)
      TestResource(s3Store, s3Store, FiniteDuration(10, "s"))
    }

  test("expose underlying metadata") { (res, log) =>
    val dir = dirUrl("expose-underlying")
    val test = for {
      (url, _) <- writeRandomFile(res.store, log)(dir)
      l        <- res.store.listAll(url)
    } yield {
      l.map { u =>
        // Note: defaultFullMetadata = true in S3Store constructor.
        val meta = u.path.representation.meta
        expect.all(
          meta.flatMap(_.contentType).isDefined,
          meta.flatMap(_.eTag).isDefined
        )
      }.combineAll
    }

    test.timeout(res.timeout)
  }

  def testUploadNoSize(store: Store[IO, S3Blob], size: Long, name: String): IO[Expectations] = {
    val url = dirUrl("multipart-upload") / name

    for {
      bytes <- randomBytes(size.toInt)
      _     <- Stream.emits(bytes).through(store.put(url, size = None)).compile.drain

      readBytes <- store.get(url, 4096).compile.to(Array)
    } yield {
      expect(readBytes sameElements bytes)
    }

  }

  test("put content with no size when aligned with multi-upload boundaries 5mb") { res =>
    testUploadNoSize(res.store, 5 * 1024 * 1024, "5mb").timeout(res.timeout)
  }

  test("put content with no size when aligned with multi-upload boundaries 10mb") { res =>
    testUploadNoSize(res.store, 10 * 1024 * 1024, "10mb").timeout(res.timeout)
  }

  test("put content with no size when aligned with multi-upload boundaries 7mb") { res =>
    testUploadNoSize(res.store, 7 * 1024 * 1024, "7mb").timeout(res.timeout)
  }

  test("put content with no size when aligned with multi-upload boundaries 12mb") { res =>
    testUploadNoSize(res.store, 12 * 1024 * 1024, "12mb").timeout(res.timeout)
  }

  test("put rotating with file-limit > bufferSize") { res =>
    val dir = dirUrl("put-rotating-s3")
    val test = for {
      data    <- randomBytes(7 * 1024 * 1024)
      counter <- IO.ref(0)
      _ <- Stream.emits(data).through(res.store.putRotate(
        counter.getAndUpdate(_ + 1).map(i => dir / s"$i"),
        6 * 1024 * 1024
      )).compile.drain
      l        <- res.store.listAll(dir, recursive = true)
      contents <- l.traverse(u => res.store.get(u, 1024).compile.to(Array))
      count    <- counter.get
    } yield {
      expect.all(
        l.size == 2,
        count == 2,
        l.flatMap(_.path.size) == List(6 * 1024 * 1024L, 1024 * 1024L),
        contents.flatten == data.toList
      )
    }
    test.timeout(res.timeout)
  }

  test("resolve type of storage class") { res =>
    val test = res.store.listAll(dirUrl("storage-class")).map { l =>
      l.map { u =>
        val sc: Option[StorageClass] = u.path.storageClass
        expect(sc.isEmpty)
      }.combineAll
    }
    test.timeout(res.timeout)
  }

}
