package blobstore.experiment.fs

import java.nio.file.{Files, Paths, StandardOpenOption, Path => JPath}

import blobstore.experiment.SingleAuthorityStore
import blobstore.experiment.url.Path
import blobstore.goRotate

import scala.jdk.CollectionConverters._
import cats.implicits._
import cats.effect.{Blocker, Concurrent, ContextShift, Resource}
import fs2.io.file.{FileHandle, WriteCursor}
import fs2.{Hotswap, Pipe, Stream}

final class FileStore[F[_]](blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F])
  extends SingleAuthorityStore[F, NioPath] {

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[NioPath]] = {
    val resolvedPath = Paths.get(path.show)
    val isDir  = Stream.eval(F.delay(Files.isDirectory( resolvedPath)))
    val isFile = Stream.eval(F.delay(Files.exists(resolvedPath)))

    val stream: Stream[F, JPath] =
      Stream
        .eval(F.delay(if (recursive) Files.walk(resolvedPath) else Files.list(resolvedPath)))
        .flatMap(x => Stream.fromIterator(x.iterator.asScala))
        .flatMap { x =>
          val dir = Files.isDirectory(x)
          if (recursive && dir) {
            Stream.empty
          } else {
            Stream.emit(x)
          }
        }

    val files = stream.map(x => Path(x.toString).as(NioPath(x)))

    val file = Stream.emit(Path(resolvedPath.toString).as(NioPath(resolvedPath))).covary[F]

    isDir.ifM(files, isFile.ifM(file, Stream.empty.covaryAll[F, Path[NioPath]]))
  }

  override def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte] = fs2.io.file.readAll[F](path, blocker, chunkSize)

  override def put[A](path: Path[A], overwrite: Boolean = true): Pipe[F, Byte, Unit] = { in =>
    val flags =
      if (overwrite) List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      else List(StandardOpenOption.CREATE_NEW)
    Stream.eval(createParentDir(path)) >> fs2.io.file.writeAll(path = path, blocker = blocker, flags = flags).apply(in)
  }

  override def move[A](src: Path[A], dst: Path[A]): F[Unit] =
    createParentDir(dst) >> F.delay(Files.move(src, dst)).void

  override def copy[A](src: Path[A], dst: Path[A]): F[Unit] =
    createParentDir(dst) >> F.delay(Files.copy(src, dst)).void

  override def remove[A](path: Path[A]): F[Unit] = F.delay {
    Files.deleteIfExists(path)
    ()
  }

  override def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] = { in =>
    val openNewFile: Resource[F, FileHandle[F]] =
      Resource
        .liftF(computePath)
        .flatTap(p => Resource.liftF(createParentDir(p)))
        .flatMap { p =>
          FileHandle.fromPath(
            path = p,
            blocker = blocker,
            flags = StandardOpenOption.CREATE :: StandardOpenOption.WRITE :: StandardOpenOption.TRUNCATE_EXISTING :: Nil
          )
        }

    def newCursor(file: FileHandle[F]): F[WriteCursor[F]] =
      WriteCursor.fromFileHandle[F](file, append = false)

    Stream
      .resource(Hotswap(openNewFile))
      .flatMap {
        case (hotswap, fileHandle) =>
          Stream.eval(newCursor(fileHandle)).flatMap { cursor =>
            goRotate(limit, 0L, in, cursor, hotswap, openNewFile)(
              c => chunk => c.writePull(chunk),
              fh => Stream.eval(newCursor(fh))
            ).stream
          }
      }
  }

  private def createParentDir[A](p: Path[A]): F[Unit] =
    F.delay(Files.createDirectories(_toNioPath(p).getParent))
      .handleErrorWith { e => F.raiseError(new Exception(s"failed to create dir: $p", e)) }
      .void

  implicit private def _toNioPath[A](path: Path[A]): JPath = {
    Paths.get(path.show)
  }
}

object FileStore {
  def apply[F[_]](blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F]): FileStore[F] =
    new FileStore(blocker)
}
