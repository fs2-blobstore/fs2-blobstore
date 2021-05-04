package blobstore

import blobstore.url.{Path, Url}
import cats.syntax.all._
import cats.effect.{IO, Ref}
import cats.effect.std.Random
import fs2.io.file.Files
import fs2.{Pipe, Stream}
import weaver.SimpleIOSuite

object StoreOpsTest extends SimpleIOSuite {

  val randomBytes: IO[Array[Byte]] = Random.scalaUtilRandom.flatMap(_.nextBytes(20))

  test("buffer contents and compute size before calling Store.put") {
    val store = new DummyStore
    val url   = Url.unsafe("foo://bucket/path/to/file.txt")
    for {
      bytes <- randomBytes
      _ <- Stream.emits(bytes).through(
        store.bufferedPut(url, overwrite = true, chunkSize = 4096)
      ).compile.drain
      m <- store.ref.get
    } yield {
      m.get(url.path.plain).map { case (callSize, bs) =>
        expect.all(callSize.contains(20), bs sameElements bytes)
      }.combineAll
    }

  }

  test("upload a file from a nio Path") {
    val store = new DummyStore
    val url   = Url.unsafe("foo://bucket/path/to/file.txt")
    Files[IO].tempFile().use { p =>
      for {
        bytes <- randomBytes
        _     <- Stream.emits(bytes).through(Files[IO].writeAll(p)).compile.drain
        _     <- Stream.eval(store.putFromNio(p, url, overwrite = true)).compile.drain
        m     <- store.ref.get
      } yield {
        m.get(url.path.plain).map { case (_, bs) =>
          expect(bs sameElements bytes)
        }.combineAll
      }
    }

  }

  test("download a file to a nio path") {
    val store = new DummyStore
    val url   = Url.unsafe("foo://bucket/path/to/file.txt")

    Files[IO].tempFile().use { nioPath =>
      for {
        bytes   <- randomBytes
        _       <- Stream.emits(bytes).through(store.put(url)).compile.drain
        _       <- store.getToNio(url, nioPath, 4096)
        written <- Files[IO].readAll(nioPath, 4096).compile.to(Array)
      } yield {
        expect(written sameElements bytes)
      }
    }

  }

  test("transfer to different store") {
    val url       = Url.unsafe("gs://foo")
    val fromStore = new DummyStore
    val toStore   = new DummyStore

    for {
      bytes <- randomBytes
      _     <- Stream.emits(bytes).through(fromStore.put(url)).compile.drain
      _     <- fromStore.transferTo(toStore, url, url)
      m     <- toStore.ref.get
    } yield {
      m.get(url.path.plain).map { case (_, bs) =>
        expect(bs sameElements bytes)
      }.combineAll
    }
  }

}

class DummyStore extends Store[IO, String] {
  val ref: Ref[IO, Map[Path.Plain, (Option[Long], Array[Byte])]] =
    Ref.unsafe[IO, Map[Path.Plain, (Option[Long], Array[Byte])]](Map.empty)
  def shouldNotBeCalled[T]: IO[T] = IO.raiseError[T](new Exception("should not be called"))

  override def put[A](url: Url[A], overwrite: Boolean, size: Option[Long] = None): Pipe[IO, Byte, Unit] =
    in => Stream.eval(in.compile.to(Array).flatMap(bytes => ref.update(_.updated(url.path.plain, (size, bytes)))))

  override def get[A](url: Url[A], chunkSize: Int): Stream[IO, Byte] =
    Stream.eval(ref.get).flatMap[IO, Byte] { m =>
      m.get(url.path.plain) match {
        case Some((_, bs)) => Stream.emits(bs.toIndexedSeq)
        case None          => Stream.raiseError[IO](new Exception("not found"))
      }
    }
  override def list[A](url: Url[A], recursive: Boolean = false): Stream[IO, Url[String]] = {
    val entries =
      ref.get.map(
        _.flatMap {
          case (k, _) =>
            if (k.toString.startsWith(url.path.plain.toString)) Some(url.withPath(k)) else None
        }.toList
      )
    Stream.eval(entries).flatMap(Stream.emits)
  }

  override def move[A, B](src: Url[A], dst: Url[B]): IO[Unit]       = shouldNotBeCalled
  override def copy[A, B](src: Url[A], dst: Url[B]): IO[Unit]       = shouldNotBeCalled
  override def remove[A](url: Url[A], recursive: Boolean): IO[Unit] = shouldNotBeCalled
  override def putRotate[A](computeUrl: IO[Url[A]], limit: Long): Pipe[IO, Byte, Unit] =
    _ => Stream.eval(shouldNotBeCalled)

  override def stat[A](url: Url[A]): Stream[IO, Url.Plain] = Stream.eval(shouldNotBeCalled)
}
