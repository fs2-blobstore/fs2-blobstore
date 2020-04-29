package blobstore

import java.nio.charset.Charset
import java.nio.file.Files
import java.util.concurrent.Executors

import cats.effect.{Blocker, IO}
import cats.effect.laws.util.TestInstances
import fs2.{Pipe, Stream}
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import implicits._
import org.scalatest.matchers.must.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class StoreOpsTest extends AnyFlatSpec with Matchers with TestInstances {

  implicit val cs = IO.contextShift(ExecutionContext.global)
  val blocker     = Blocker.liftExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))

  behavior of "PutOps"
  it should "buffer contents and compute size before calling Store.put" in {
    val bytes: Array[Byte] = "AAAAAAAAAA".getBytes(Charset.forName("utf-8"))
    val store              = DummyStore(_.size must be(Some(bytes.length)))

    Stream
      .emits(bytes)
      .covary[IO]
      .through(store.bufferedPut(Path("path/to/file.txt"), blocker))
      .compile
      .drain
      .unsafeRunSync()
    store.buf.toArray must be(bytes)

  }

  it should "upload a file from a nio Path" in {
    val bytes = "hello".getBytes(Charset.forName("utf-8"))
    val store = DummyStore(_.size must be(Some(bytes.length)))

    Stream
      .bracket(IO(Files.createTempFile("test-file", ".bin"))) { p => IO(p.toFile.delete).void }
      .flatMap { p =>
        Stream.emits(bytes).covary[IO].through(fs2.io.file.writeAll(p, blocker)).drain ++
          Stream.eval(store.put(p, Path("path/to/file.txt"), blocker))
      }
      .compile
      .drain
      .unsafeRunSync()
    store.buf.toArray must be(bytes)
  }

  it should "download a file to a nio path" in {
    val bytes = "hello".getBytes(Charset.forName("utf-8"))
    val store = DummyStore(_ => succeed)
    val path  = Path("path/to/file.txt")
    Stream.emits(bytes).through(store.put(path)).compile.drain.unsafeRunSync()

    Stream
      .bracket(IO(Files.createTempFile("test-file", ".bin")))(p => IO(p.toFile.delete).void)
      .flatMap { nioPath =>
        Stream.eval(store.get(path, nioPath, blocker)) >> Stream.eval {
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

final case class DummyStore(check: Path => Assertion) extends Store[IO] {
  val buf = new ArrayBuffer[Byte]()
  override def put(path: Path, overwrite: Boolean): Pipe[IO, Byte, Unit] = {
    check(path)
    in => {
      buf.appendAll(in.compile.toVector.unsafeRunSync())
      Stream.emit(())
    }
  }
  override def get(path: Path, chunkSize: Int): Stream[IO, Byte]                   = Stream.emits(buf)
  override def list(path: Path, recursive: Boolean = false): Stream[IO, Path]      = ???
  override def move(src: Path, dst: Path): IO[Unit]                                = ???
  override def copy(src: Path, dst: Path): IO[Unit]                                = ???
  override def remove(path: Path): IO[Unit]                                        = ???
  override def putRotate(computePath: IO[Path], limit: Long): Pipe[IO, Byte, Unit] = ???
}
