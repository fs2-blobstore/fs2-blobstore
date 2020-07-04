package blobstore.experiment.fs

import java.nio.file.{Files, Paths, StandardOpenOption}

import blobstore.experiment.SingleAuthorityStore

import scala.jdk.CollectionConverters._
import cats.implicits._
import cats.effect.{Blocker, Concurrent, ContextShift, Resource}
import fs2.io.file.{FileHandle, WriteCursor}
import fs2.{Hotswap, Pipe, Stream}

final class FileStore[F[_]](blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F])
  extends SingleAuthorityStore[F, NioPath] {
  val absRoot: String = fsroot.toAbsolutePath.normalize.toString

  override def list(path: Path, recursive: Boolean = false): Stream[F, Path] = {
    val isDir  = Stream.eval(F.delay(Files.isDirectory(path)))
    val isFile = Stream.eval(F.delay(Files.exists(path)))

    val stream: Stream[F, (NioPath, Boolean)] =
      Stream
        .eval(F.delay(if (recursive) Files.walk(path) else Files.list(path)))
        .flatMap(x => Stream.fromIterator(x.iterator.asScala))
        .flatMap { x =>
          val dir = Files.isDirectory(x)
          if (recursive && dir) {
            Stream.empty
          } else {
            Stream.emit(x -> dir)
          }
        }

    val files = stream
      .evalMap {
        case (x, dir) =>
          F.delay {
            Path(x.toAbsolutePath.toString.replaceFirst(absRoot, "") ++ (if (dir) "/" else ""))
              .withSize(Option(Files.size(x)), reset = false)
              .withLastModified(Option(Files.getLastModifiedTime(path).toInstant), reset = false)
              .withIsDir(Option(dir), reset = false)
          }
      }

    val file = Stream.eval {
      F.delay {
        path
          .withSize(Option(Files.size(path)), reset = false)
          .withLastModified(Option(Files.getLastModifiedTime(path).toInstant), reset = false)
      }
    }

    isDir.ifM(files, isFile.ifM(file, Stream.empty.covaryAll[F, Path]))
  }

  override def get(path: Path, chunkSize: Int): Stream[F, Byte] = fs2.io.file.readAll[F](path, blocker, chunkSize)

  override def put(path: Path, overwrite: Boolean = true): Pipe[F, Byte, Unit] = { in =>
    val flags =
      if (overwrite) List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      else List(StandardOpenOption.CREATE_NEW)
    Stream.eval(createParentDir(path)) >> fs2.io.file.writeAll(path = path, blocker = blocker, flags = flags).apply(in)
  }

  override def move(src: Path, dst: Path): F[Unit] =
    createParentDir(dst) >> F.delay(Files.move(src, dst)).void

  override def copy(src: Path, dst: Path): F[Unit] =
    createParentDir(dst) >> F.delay(Files.copy(src, dst)).void

  override def remove(path: Path): F[Unit] = F.delay {
    Files.deleteIfExists(path)
    ()
  }

  override def putRotate(computePath: F[Path], limit: Long): Pipe[F, Byte, Unit] = { in =>
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

  private def createParentDir(p: Path): F[Unit] =
    F.delay(Files.createDirectories(_toNioPath(p).getParent))
      .handleErrorWith { e => F.raiseError(new Exception(s"failed to create dir: $p", e)) }
      .void

  implicit private def _toNioPath(path: Path): NioPath = {
    val withRoot = path.root.fold(path.pathFromRoot)(path.pathFromRoot.prepend)
    val withName = path.fileName.fold(withRoot)(withRoot.append)
    Paths.get(absRoot, withName.toList: _*)
  }
}

object FileStore {
  def apply[F[_]](fsroot: NioPath, blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F]): FileStore[F] =
    new FileStore(fsroot, blocker)
}
