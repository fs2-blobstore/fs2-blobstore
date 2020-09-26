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

import java.nio.file.{Files, Paths, StandardOpenOption, Path => JPath}

import blobstore.url.{Authority, Hostname, Path}

import scala.jdk.CollectionConverters._
import cats.implicits._
import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync}
import fs2.io.file.{FileHandle, WriteCursor}
import fs2.{Hotswap, Pipe, Stream}

final class LocalStore[F[_]](blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F])
  extends FileStore[F, NioPath] {

  override def authority: Authority = Authority.Standard(Hostname.unsafe("localhost"), None, None)

  override def list(path: Path.Plain, recursive: Boolean = false): Stream[F, Path[NioPath]] = {
    val isDir  = Stream.eval(F.delay(Files.isDirectory(path)))
    val isFile = Stream.eval(F.delay(Files.exists(path)))

    val stream: Stream[F, (JPath, Boolean)] =
      Stream
        .eval(F.delay(if (recursive) Files.walk(path) else Files.list(path)))
        .flatMap(x => Stream.fromIterator(x.iterator.asScala))
        .flatMap { x =>
          val isDir = Files.isDirectory(x)
          if (recursive && isDir) {
            Stream.empty
          } else {
            Stream.emit(x -> isDir)
          }
        }

    val files = stream
      .evalMap {
        case (x, isDir) =>
          F.delay(NioPath(x, Option(Files.size(x)), isDir, Option(Files.getLastModifiedTime(path).toInstant)))
      }

    val file = Stream.eval {
      F.delay {
        NioPath(path = JPath.of(path.show), size = Option(Files.size(path)), isDir = false, lastModified = Option(Files.getLastModifiedTime(path).toInstant))
      }
    }

    isDir.ifM(files, isFile.ifM(file, Stream.empty.covaryAll[F, NioPath])).map(p => Path.of(p.path.toString, p))
  }

  override def get(path: Path.Plain, chunkSize: Int): Stream[F, Byte] = fs2.io.file.readAll[F](path, blocker, chunkSize)

  override def put(path: Path.Plain, overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = { in =>
    val flags =
      if (overwrite) List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      else List(StandardOpenOption.CREATE_NEW)
    Stream.eval(createParentDir(path)) >> fs2.io.file.writeAll(path = path, blocker = blocker, flags = flags).apply(in)
  }

  override def move(src: Path.Plain, dst: Path.Plain): F[Unit] =
    createParentDir(dst) >> F.delay(Files.move(src, dst)).void

  override def copy(src: Path.Plain, dst: Path.Plain): F[Unit] =
    createParentDir(dst) >> F.delay(Files.copy(src, dst)).void

  override def remove(path: Path.Plain): F[Unit] = F.delay {
    Files.deleteIfExists(path)
    ()
  }

  override def putRotate(computePath: F[Path.Plain], limit: Long): Pipe[F, Byte, Unit] = { in =>
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

  private def createParentDir(p: Path.Plain): F[Unit] =
    F.delay(Files.createDirectories(_toJPath(p).getParent))
      .handleErrorWith { e => F.raiseError(new Exception(s"failed to create dir: $p", e)) }
      .void

  implicit private def _toJPath(path: Path.Plain): JPath =
    Paths.get(path.show)

  override def liftAuthority: Store[F, Authority.Standard, NioPath] =
    new Store.DelegatingStore[F, NioPath, NioPath](identity, Right(this))
}

object LocalStore {
  def apply[F[_]](blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F]): LocalStore[F] =
    new LocalStore(blocker)
}
