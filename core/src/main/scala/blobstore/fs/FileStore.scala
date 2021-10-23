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

import blobstore.url.Path.Plain
import blobstore.url.{FsObject, Path, Url}
import cats.data.Validated
import cats.effect.{Blocker, Concurrent, ContextShift, Resource}
import cats.syntax.all.*
import fs2.io.file.{FileHandle, WriteCursor}
import fs2.{Hotswap, Pipe, Stream}

import scala.jdk.CollectionConverters.*
import java.nio.file.{Files, Path as JPath, Paths, StandardOpenOption}
import scala.util.Try

class FileStore[F[_]: Concurrent: ContextShift](blocker: Blocker) extends PathStore[F, NioPath] {

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[NioPath]] = {
    val p = path.nioPath

    val isDir  = Stream.eval(Concurrent[F].delay(Files.isDirectory(p)))
    val isFile = Stream.eval(Concurrent[F].delay(Files.exists(p)))

    val stream: Stream[F, (JPath, Boolean)] =
      Stream
        .eval(Concurrent[F].delay(if (recursive) Files.walk(p) else Files.list(p)))
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
          Concurrent[F].delay(NioPath(
            x,
            Try(Files.size(x)).toOption,
            isDir,
            Try(Files.getLastModifiedTime(p)).toOption.map(_.toInstant)
          ))
      }

    val file = Stream.eval {
      Concurrent[F].delay {
        NioPath(
          path = Paths.get(path.show),
          size = Try(Files.size(p)).toOption,
          isDir = false,
          lastModified = Try(Files.getLastModifiedTime(p)).toOption.map(_.toInstant)
        )
      }
    }

    isDir.ifM(files, isFile.ifM(file, Stream.empty.covaryAll[F, NioPath])).map(p => Path.of(p.path.toString, p))
  }

  override def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte] =
    fs2.io.file.readAll[F](path.nioPath, blocker, chunkSize)

  override def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = {
    in =>
      val flags =
        if (overwrite) List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        else List(StandardOpenOption.CREATE_NEW)
      Stream.eval(createParentDir(path.plain)) >> fs2.io.file.writeAll(
        path = path.nioPath,
        blocker = blocker,
        flags = flags
      ).apply(
        in
      )
  }

  override def move[A, B](src: Path[A], dst: Path[B]): F[Unit] =
    createParentDir(dst.plain) >> Concurrent[F].delay(Files.move(src.nioPath, dst.nioPath)).void

  override def copy[A, B](src: Path[A], dst: Path[B]): F[Unit] =
    createParentDir(dst.plain) >> Concurrent[F].delay(Files.copy(src.nioPath, dst.nioPath)).void

  override def remove[A](path: Path[A], recursive: Boolean = false): F[Unit] =
    if (recursive) {
      def recurse(path: JPath): Stream[F, JPath] =
        fs2.io.file.directoryStream(blocker, path).flatMap {
          p =>
            (if (Files.isDirectory(p)) recurse(p) else Stream.empty) ++ Stream.emit(p)
        }
      recurse(path.nioPath).evalMap(p => blocker.delay(Files.deleteIfExists(p))).compile.drain
    } else blocker.delay(Files.deleteIfExists(path.nioPath)).void

  override def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] = { in =>
    val openNewFile: Resource[F, FileHandle[F]] =
      Resource
        .eval(computePath)
        .flatTap(p => Resource.eval(createParentDir(p.plain)))
        .flatMap { p =>
          FileHandle.fromPath(
            path = p.nioPath,
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
    Concurrent[F].delay(Files.createDirectories(p.nioPath.getParent))
      .handleErrorWith { e => Concurrent[F].raiseError(new Exception(s"failed to create dir: $p", e)) }
      .void

  private def nioStat(p: JPath): F[NioPath] = (
    blocker.delay(Try(Files.size(p)).toOption),
    blocker.delay(Try(Files.isDirectory(p)).toOption.getOrElse(p.toString.endsWith("/"))),
    blocker.delay(Try(Files.getLastModifiedTime(p)).toOption.map(_.toInstant))
  ).mapN((size, isDir, time) => NioPath(p, size, isDir, time))

  override def stat[A](path: Path[A]): F[Option[Path[NioPath]]] = {
    val p = path.nioPath

    if (!Files.exists(p)) none[Path[NioPath]].pure
    else nioStat(p).map(nioPath => path.as(nioPath).some)
  }

  /** Lifts this FileStore to a Store accepting URLs and exposing blobs of type `B`. You must provide a mapping from
    * this Store's BlobType to B, and you may provide a function `g` for controlling input paths to this store.
    *
    * Input URLs to the returned store are validated against this Store's authority before the path is extracted and
    * passed to this store.
    */
  override def lift(g: Url.Plain => Validated[Throwable, Plain]): Store[F, NioPath] =
    new Store.DelegatingStore[F, NioPath](this, g)

  override def getContents[A](path: Path[A], chunkSize: Int): F[String] =
    get(path, chunkSize).through(fs2.text.utf8Decode).compile.string

  override def transferTo[B, P, U](
    dstStore: Store[F, B],
    srcPath: Path[P],
    dstUrl: Url[U]
  )(implicit ev: B <:< FsObject): F[Int] = defaultTransferTo(this, dstStore, srcPath, dstUrl)
}

object FileStore {
  def apply[F[_]: Concurrent: ContextShift](blocker: Blocker): FileStore[F] = new FileStore(blocker)
}
