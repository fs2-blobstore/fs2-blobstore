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
import cats.effect.Async
import cats.syntax.all.*
import fs2.{Pipe, Stream}
import fs2.io.file.{Files, Flags, Path as Fs2Path}

import java.nio.file.{Files as JFiles, StandardOpenOption}

class FileStore[F[_]: Files: Async] extends PathStore[F, NioPath] {

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[NioPath]] = {
    val p = Fs2Path.fromNioPath(path.nioPath)

    val isDirStream  = Stream.eval(Files[F].isDirectory(p))
    val isFileStream = Stream.eval(Files[F].isRegularFile(p))

    val stream =
      if (recursive) Files[F].walk(p).evalFilterNot(p => Files[F].isDirectory(p))
      else Files[F].list(p)

    isDirStream.ifM(
      stream,
      isFileStream.ifM(Stream.emit(p), Stream.empty)
    ).evalMap(fs2Path => nioStat(fs2Path).map(p => Path.of(p.path.toString, p)))
  }

  override def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte] =
    Files[F].readAll(Fs2Path.fromNioPath(path.nioPath), chunkSize, Flags.Read)

  override def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = {
    in =>
      val flags =
        if (overwrite) List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        else List(StandardOpenOption.CREATE_NEW)
      Stream.eval(createParentDir(path.plain)) >> Files[F].writeAll(
        path = Fs2Path.fromNioPath(path.nioPath),
        flags = Flags.fromOpenOptions(flags)
      ).apply(
        in
      )
  }

  override def move[A, B](src: Path[A], dst: Path[B]): F[Unit] =
    createParentDir(dst.plain) >> Files[F].move(Fs2Path.fromNioPath(src.nioPath), Fs2Path.fromNioPath(dst.nioPath)).void

  override def copy[A, B](src: Path[A], dst: Path[B]): F[Unit] =
    createParentDir(dst.plain) >> Files[F].copy(Fs2Path.fromNioPath(src.nioPath), Fs2Path.fromNioPath(dst.nioPath)).void

  override def remove[A](path: Path[A], recursive: Boolean = false): F[Unit] = {
    val p = Fs2Path.fromNioPath(path.nioPath)
    if (recursive) {
      Files[F].isDirectory(p).ifM(Files[F].deleteRecursively(p), Files[F].deleteIfExists(p).void)
    } else {
      Files[F].deleteIfExists(p).void
    }
  }

  override def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] =
    Files[F].writeRotate(
      computePath.map(p => Fs2Path.fromNioPath(p.nioPath)),
      limit,
      Flags.fromOpenOptions(
        StandardOpenOption.CREATE :: StandardOpenOption.WRITE :: StandardOpenOption.TRUNCATE_EXISTING :: Nil
      )
    )

  private def createParentDir(p: Path.Plain): F[Unit] =
    Files[F].createDirectories(Fs2Path.fromNioPath(p.nioPath.getParent)).void

  private def nioStat(p: Fs2Path): F[NioPath] = (
    Files[F].size(p).attempt.map(_.toOption),
    Files[F].isDirectory(p).attempt.map(_.toOption.getOrElse(p.toString.endsWith("/"))),
    Async[F].blocking(JFiles.getLastModifiedTime(p.toNioPath)).attempt.map(_.toOption.map(_.toInstant))
  ).mapN((size, isDir, time) => NioPath(p.toNioPath, size, isDir, time))

  override def stat[A](path: Path[A]): F[Option[Path[NioPath]]] = {
    val p = Fs2Path.fromNioPath(path.nioPath)

    Files[F].exists(p).ifM(
      nioStat(p).map(nioPath => path.as(nioPath).some),
      none.pure
    )
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
    get(path, chunkSize).through(fs2.text.utf8.decode).compile.string

  override def transferTo[B, P, U](
    dstStore: Store[F, B],
    srcPath: Path[P],
    dstUrl: Url[U]
  )(implicit ev: B <:< FsObject): F[Int] = defaultTransferTo(this, dstStore, srcPath, dstUrl)
}

object FileStore {
  def apply[F[_]: Files: Async]: FileStore[F] = new FileStore
}
