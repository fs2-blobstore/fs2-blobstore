/*
Copyright 2018 LendUp Global, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package blobstore
package fs

import java.nio.file.{Files, Paths, StandardOpenOption, Path => NioPath}
import java.util.Date

import scala.jdk.CollectionConverters._
import cats.implicits._
import cats.effect.{Blocker, Concurrent, ContextShift, Resource}
import fs2.io.file.{createDirectories, FileHandle, WriteCursor}
import fs2.{Chunk, Hotswap, Pipe, Stream}

final class FileStore[F[_]](fsroot: NioPath, blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F])
  extends Store[F] {
  val absRoot: String = fsroot.toAbsolutePath.normalize.toString

  override def list(path: Path): Stream[F, Path] = {
    val isDir  = Stream.eval(F.delay(Files.isDirectory(path)))
    val isFile = Stream.eval(F.delay(Files.exists(path)))

    val files = Stream
      .eval(F.delay(Files.list(path)))
      .flatMap(x => Stream.fromIterator(x.iterator.asScala))
      .evalMap(x =>
        F.delay(
          Path(x.toAbsolutePath.toString.replaceFirst(absRoot, "")).copy(
            size = Option(Files.size(x)),
            isDir = Files.isDirectory(x),
            lastModified = Option(new Date(Files.getLastModifiedTime(path).toMillis))
          )
        )
      )

    val file = Stream.eval {
      F.delay {
        path.copy(
          size = Option(Files.size(path)),
          lastModified = Option(new Date(Files.getLastModifiedTime(path).toMillis))
        )
      }
    }

    isDir.ifM(files, isFile.ifM(file, Stream.empty.covaryAll[F, Path]))
  }

  override def get(path: Path, chunkSize: Int): Stream[F, Byte] = fs2.io.file.readAll[F](path, blocker, chunkSize)

  override def put(path: Path): Pipe[F, Byte, Unit] = { in =>
    val mkdir = Stream.eval(createDirectories(blocker, _toNioPath(path).getParent).as(true))
    mkdir.ifM(
      fs2.io.file.writeAll(path, blocker).apply(in),
      Stream.raiseError[F](new Exception(s"failed to create dir: $path"))
    )
  }

  override def move(src: Path, dst: Path): F[Unit] =
    F.delay {
      Files.createDirectories(_toNioPath(dst).getParent)
      Files.move(src, dst)
    }.void

  override def copy(src: Path, dst: Path): F[Unit] = {
    F.delay {
      Files.createDirectories(_toNioPath(dst).getParent)
      Files.copy(src, dst)
    }.void
  }

  override def remove(path: Path): F[Unit] = F.delay {
    Files.deleteIfExists(path)
    ()
  }

  override def putRotate(computePath: F[Path], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, FileHandle[F]] =
      Resource
        .liftF(computePath.map(_toNioPath))
        .flatMap { p =>
          Resource.liftF(createDirectories(blocker, p.getParent)) *>
            FileHandle.fromPath(p, blocker, StandardOpenOption.CREATE :: StandardOpenOption.WRITE :: Nil)
        }

    def newCursor(file: FileHandle[F]): F[WriteCursor[F]] =
      WriteCursor.fromFileHandle[F](file, append = false)

    in =>
      Stream
        .resource(Hotswap(openNewFile))
        .flatMap {
          case (hotswap, fileHandle) =>
            Stream.eval(newCursor(fileHandle)).flatMap { cursor =>
              goRotate(limit, 0L, in, cursor, hotswap, openNewFile)(
                c => arr => c.writePull(Chunk.bytes(arr)),
                fh => Stream.eval(newCursor(fh))
              ).stream
            }
        }
  }

  implicit private def _toNioPath(path: Path): NioPath =
    Paths.get(absRoot, path.root, path.key)

}

object FileStore {
  def apply[F[_]](fsroot: NioPath, blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F]): FileStore[F] =
    new FileStore(fsroot, blocker)
}
