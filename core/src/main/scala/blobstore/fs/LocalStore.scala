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

import blobstore.Store.BlobStore
import blobstore.url.{Authority, Hostname, Path}
import blobstore.url.Path.Plain

import scala.jdk.CollectionConverters._
import cats.syntax.all._
import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync}
import cats.Show
import fs2.io.file.{FileHandle, WriteCursor}
import fs2.{Hotswap, Pipe, Stream}

import scala.util.Try

final class LocalStore[F[_]](blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F])
  extends FileStore[F, NioPath] {

  override def authority: Authority = Authority.Standard(Hostname.unsafe("localhost"), None, None)

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[NioPath]] = {
    val isDir  = Stream.eval(F.delay(Files.isDirectory(path.plain)))
    val isFile = Stream.eval(F.delay(Files.exists(path.plain)))

    val stream: Stream[F, (JPath, Boolean)] =
      Stream
        .eval(F.delay(if (recursive) Files.walk(path.plain) else Files.list(path.plain)))
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
          F.delay(NioPath(x, Try(Files.size(x)).toOption, isDir, Try(Files.getLastModifiedTime(path.plain)).toOption.map(_.toInstant)))
      }

    val file = Stream.eval {
      F.delay {
        NioPath(
          path = Paths.get(path.show),
          size = Try(Files.size(path.plain)).toOption,
          isDir = false,
          lastModified = Try(Files.getLastModifiedTime(path.plain)).toOption.map(_.toInstant)
        )
      }
    }

    isDir.ifM(files, isFile.ifM(file, Stream.empty.covaryAll[F, NioPath])).map(p => Path.of(p.path.toString, p))
  }

  override def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte] =
    fs2.io.file.readAll[F](path.plain, blocker, chunkSize)

  override def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = {
    in =>
      val flags =
        if (overwrite) List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        else List(StandardOpenOption.CREATE_NEW)
      Stream.eval(createParentDir(path.plain)) >> fs2.io.file.writeAll(path = path.plain, blocker = blocker, flags = flags).apply(
        in
      )
  }

  override def move[A, B](src: Path[A], dst: Path[B]): F[Unit] =
    createParentDir(dst.plain) >> F.delay(Files.move(src.plain, dst.plain)).void

  override def copy[A, B](src: Path[A], dst: Path[B]): F[Unit] =
    createParentDir(dst.plain) >> F.delay(Files.copy(src.plain, dst.plain)).void

  override def remove[A](path: Path[A]): F[Unit] = {
    val s = if (isDir(path))
      list(path).map(remove).map(Stream.eval).parJoinUnbounded ++ Stream.eval(F.delay(Files.deleteIfExists(path.plain)))
    else Stream.eval(F.delay(Files.deleteIfExists(path.plain)))

    s.compile.drain
  }

  override def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] = { in =>
    val openNewFile: Resource[F, FileHandle[F]] =
      Resource
        .liftF(computePath)
        .flatTap(p => Resource.liftF(createParentDir(p.plain)))
        .flatMap { p =>
          FileHandle.fromPath(
            path = p.plain,
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
    new Store.DelegatingStore[F, NioPath, Authority.Standard, NioPath](identity, Right(this))

  override def stat[A](path: Path[A]): Stream[F, Option[Path[NioPath]]] =
    Stream.eval(Sync[F].delay {
      val p = Paths.get(path.show)

      if (!Files.exists(p)) None else
        path.as(NioPath(
          p,
          Try(Files.size(p)).toOption,
          Try(Files.isDirectory(p)).toOption.getOrElse(isDir(path)),
          Try(Files.getLastModifiedTime(path.plain)).toOption.map(_.toInstant)
        )).some
    })

  // The local file system can't have file names ending with slash
  private def isDir[A](path: Path[A]): Boolean = path.show.endsWith("/")
}

object LocalStore {
  def apply[F[_]](blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F]): LocalStore[F] =
    new LocalStore(blocker)
}
