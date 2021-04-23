package blobstore

import blobstore.url.{Path, Url}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file.Files
import fs2.{Pipe, Stream}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import java.nio.charset.{Charset, StandardCharsets}
import scala.collection.mutable.ArrayBuffer

class StoreOpsTest extends AnyFlatSpec with Matchers {

  behavior of "PutOps"
  it should "buffer contents and compute size before calling Store.put" in {
    val bytes: Array[Byte] = "AAAAAAAAAA".getBytes(Charset.forName("utf-8"))
    val store              = DummyStore()

    Stream
      .emits(bytes)
      .covary[IO]
      .through(store.bufferedPut(Url.unsafe("foo://bucket/path/to/file.txt"), true, 4096))
      .compile
      .drain
      .unsafeRunSync()

    store.buf.toArray must be(bytes)
  }

  it should "upload a file from a nio Path" in {
    val bytes = "hello".getBytes(Charset.forName("utf-8"))
    val store = DummyStore()

    Stream
      .resource(Files[IO].tempFile())
      .flatMap { p =>
        Stream.emits(bytes).covary[IO].through(Files[IO].writeAll(p)).drain ++
          Stream.eval(store.putFromNio(p, Url.unsafe("foo://bucket/path/to/file.txt"), true))
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
      .resource(Files[IO].tempFile())
      .flatMap { nioPath =>
        Stream.eval(store.getToNio(path, nioPath, 4096)) >> Stream.eval {
          Files[IO].readAll(nioPath, 4096).compile.to(Array).map(_ mustBe bytes)
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

final case class DummyStore() extends Store[IO, String] {
  val buf = new ArrayBuffer[Byte]()
  override def put[A](url: Url[A], overwrite: Boolean, size: Option[Long] = None): Pipe[IO, Byte, Unit] = {
    in =>
      {
        buf.appendAll(in.compile.toVector.unsafeRunSync())
        Stream.emit(())
      }
  }
  override def get[A](url: Url[A], chunkSize: Int): Stream[IO, Byte] = Stream.emits(buf)
  override def list[A](url: Url[A], recursive: Boolean = false): Stream[IO, Url.Plain] =
    Stream.emits(List(url.withPath(Path("the-file.txt"))))
  override def move[A, B](src: Url[A], dst: Url[B]): IO[Unit]                          = ???
  override def copy[A, B](src: Url[A], dst: Url[B]): IO[Unit]                          = ???
  override def remove[A](url: Url[A], recursive: Boolean): IO[Unit]                    = ???
  override def putRotate[A](computeUrl: IO[Url[A]], limit: Long): Pipe[IO, Byte, Unit] = ???

  override def stat[A](url: Url[A]): Stream[IO, Url.Plain] = ???
}

object DummyStore {
  def withContents(s: String): DummyStore = {
    val store = DummyStore()
    store.buf.appendAll(s.getBytes(StandardCharsets.UTF_8))
    store
  }
}
