package blobstore

import blobstore.url.{Path, Url}
import cats.effect.{Blocker, ContextShift, IO}
import fs2.{Pipe, Stream}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.Files
import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class StoreOpsTest extends AnyFlatSpec with Matchers {

  implicit val cs = IO.contextShift(ExecutionContext.global)
  val blocker     = Blocker.liftExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))

  behavior of "PutOps"
  it should "buffer contents and compute size before calling Store.put" in {
    val bytes: Array[Byte] = "AAAAAAAAAA".getBytes(Charset.forName("utf-8"))
    val store              = DummyStore()

    Stream
      .emits(bytes)
      .covary[IO]
      .through(store.bufferedPut(Url.unsafe("foo://bucket/path/to/file.txt"), true, 4096, blocker))
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
          Stream.eval(store.put(p, Url.unsafe("foo://bucket/path/to/file.txt"), true, blocker))
      }
      .compile
      .drain
      .unsafeRunSync()
    store.buf.toArray must be(bytes)
  }

  it should "download a file to a nio path" in {
    val bytes = "hello".getBytes(Charset.forName("utf-8"))
    val store = DummyStore()
    val path  = Url.unsafe("foo://bucket/path/to/file.txt")
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

  it should "transferTo" in {
    val u1   = Url.unsafe("gs://foo")
    val from = DummyStore.withContents("foo")
    val to   = DummyStore()

    from.transferTo(to, u1, u1).unsafeRunSync()

    val result = new String(to.buf.toArray, StandardCharsets.UTF_8)

    result mustBe "foo"
  }
}

final case class DummyStore()(implicit cs: ContextShift[IO]) extends Store[IO, String] {
  val buf = new ArrayBuffer[Byte]()
  override def put(url: Url, overwrite: Boolean, size: Option[Long] = None): Pipe[IO, Byte, Unit] = {
    in =>
      {
        buf.appendAll(in.compile.toVector.unsafeRunSync())
        Stream.emit(())
      }
  }
  override def get(url: Url, chunkSize: Int): Stream[IO, Byte] = Stream.emits(buf)
  override def list(url: Url, recursive: Boolean = false): Stream[IO, Path.Plain] =
    Stream.emits(List(Path("the-file.txt")))
  override def move(src: Url, dst: Url): IO[Unit]                                 = ???
  override def copy(src: Url, dst: Url): IO[Unit]                                 = ???
  override def remove(url: Url, recursive: Boolean): IO[Unit]                     = ???
  override def putRotate(computePath: IO[Url], limit: Long): Pipe[IO, Byte, Unit] = ???

  override def stat(url: Url): Stream[IO, Path[String]] = ???
}

object DummyStore {
  def withContents(s: String)(implicit cs: ContextShift[IO]): DummyStore = {
    val store = DummyStore()
    store.buf.appendAll(s.getBytes(StandardCharsets.UTF_8))
    store
  }
}
