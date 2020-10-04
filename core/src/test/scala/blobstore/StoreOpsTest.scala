package blobstore

import java.nio.charset.Charset
import java.nio.file.Files
import java.time.Instant
import java.util.concurrent.Executors

import blobstore.url.{Authority, FileSystemObject, Path, Url}
import blobstore.url.Authority.Bucket
import blobstore.Store.UniversalStore
import blobstore.url.general.GeneralStorageClass
import cats.effect.{Blocker, ContextShift, IO}
import cats.effect.laws.util.TestInstances
import fs2.{Pipe, Stream}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class StoreOpsTest extends AnyFlatSpec with Matchers with TestInstances {

  implicit val cs = IO.contextShift(ExecutionContext.global)
  val blocker     = Blocker.liftExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))

  implicit val fso: FileSystemObject[String] = new FileSystemObject[String] {
    override def name(a: String): String = ???

    override def size(a: String): Option[Long] = ???

    override def isDir(a: String): Boolean = ???

    override def lastModified(a: String): Option[Instant] = ???

    override def storageClass(a: String): Option[GeneralStorageClass] = ???
  }

  behavior of "PutOps"
  it should "buffer contents and compute size before calling Store.put" in {
    val bytes: Array[Byte] = "AAAAAAAAAA".getBytes(Charset.forName("utf-8"))
    val store              = DummyStore()

    Stream
      .emits(bytes)
      .covary[IO]
      .through(store.bufferedPut(Url.unsafe[Bucket]("foo://bucket/path/to/file.txt"), true, 4096, blocker))
      .compile
      .drain
      .unsafeRunSync()

    store.buf.toArray must be(bytes)
  }

  it should "upload a file from a nio Path" in {
    val bytes = "hello".getBytes(Charset.forName("utf-8"))
    val store = DummyStore()

    Stream
      .bracket(IO(Files.createTempFile("test-file", ".bin"))) { p => IO(p.toFile.delete).void }
      .flatMap { p =>
        Stream.emits(bytes).covary[IO].through(fs2.io.file.writeAll(p, blocker)).drain ++
          Stream.eval(store.put(p, Url.unsafe[Bucket]("foo://bucket/path/to/file.txt"), true, blocker))
      }
      .compile
      .drain
      .unsafeRunSync()
    store.buf.toArray must be(bytes)
  }

  it should "download a file to a nio path" in {
    val bytes = "hello".getBytes(Charset.forName("utf-8"))
    val store = DummyStore()
    val path  = Url.unsafe[Bucket]("foo://bucket/path/to/file.txt")
    Stream.emits(bytes).through(store.put(path)).compile.drain.unsafeRunSync()

    Stream
      .bracket(IO(Files.createTempFile("test-file", ".bin")))(p => IO(p.toFile.delete).void)
      .flatMap { nioPath =>
        Stream.eval(store.get(path, nioPath, 4096, blocker)) >> Stream.eval {
          IO {
            Files.readAllBytes(nioPath) mustBe bytes
          }
        }
      }
      .compile
      .drain
      .unsafeRunSync()
  }
}

final case class DummyStore()(implicit cs: ContextShift[IO]) extends Store[IO, Authority.Bucket, String] {
  val buf = new ArrayBuffer[Byte]()
  override def put(url: Url[Bucket], overwrite: Boolean, size: Option[Long] = None): Pipe[IO, Byte, Unit] = {
    in =>
      {
        buf.appendAll(in.compile.toVector.unsafeRunSync())
        Stream.emit(())
      }
  }
  override def get(url: Url[Authority.Bucket], chunkSize: Int): Stream[IO, Byte]                    = Stream.emits(buf)
  override def list(url: Url[Authority.Bucket], recursive: Boolean = false): Stream[IO, Path.Plain] = ???
  override def move(src: Url[Bucket], dst: Url[Bucket]): IO[Unit]                                   = ???
  override def copy(src: Url[Bucket], dst: Url[Bucket]): IO[Unit]                                   = ???
  override def remove(url: Url[Bucket], recursive: Boolean): IO[Unit]                               = ???
  override def putRotate(computePath: IO[Url[Bucket]], limit: Long): Pipe[IO, Byte, Unit]           = ???

  override def stat(url: Url[Bucket]): IO[Option[Path[String]]] = ???

  override def liftToUniversal: UniversalStore[IO] = ???
}
