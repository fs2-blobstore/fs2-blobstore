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

import blobstore.url.{FsObject, Path, Url}
import cats.MonadError
import cats.data.Validated
import cats.effect.Concurrent
import cats.syntax.all._
import fs2.io.file.Files
import fs2.{Pipe, Stream}

trait Store[F[_], +BlobType] {

  /** @param url to list
    * @param recursive when true returned list would contain files at given path and all sub-folders but no folders,
    *                  otherwise – return files and folder at given path.
    * @return stream of Paths. Implementing stores must guarantee that returned Paths
    *         have correct values for size, isDir and lastModified.
    * @example Given Path pointing at folder:
    *          folder/a
    *          folder/b
    *          folder/c
    *          folder/sub-folder/d
    *          folder/sub-folder/sub-sub-folder/e
    *
    *          list(folder, recursive = true)  -> [a, b, c, d, e]
    *          list(folder, recursive = false) -> [a, b, c, sub-folder]
    */
  def list[A](url: Url[A], recursive: Boolean = false): Stream[F, Url[BlobType]]

  /** @param url to get
    * @param chunkSize bytes to read in each chunk.
    * @return stream of bytes
    */
  def get[A](url: Url[A], chunkSize: Int): Stream[F, Byte]

  /** @param url to put
    * @param overwrite when true putting to path with pre-existing file would overwrite the content, otherwise – fail with error.
    * @return sink of bytes
    */
  def put[A](url: Url[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit]

  /** Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def move[A, B](src: Url[A], dst: Url[B]): F[Unit]

  /** Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def copy[A, B](src: Url[A], dst: Url[B]): F[Unit]

  /** Remove bytes for given path. Call should succeed even if there is nothing stored at that path.
    * @param url to remove
    * @return F[Unit]
    */
  def remove[A](url: Url[A], recursive: Boolean = false): F[Unit]

  /** Writes all data to a sequence of blobs/files, each limited in size to `limit`.
    *
    * The `computeUrl` operation is used to compute the path of the first file
    * and every subsequent file. Typically, the next file should be determined
    * by analyzing the current state of the filesystem -- e.g., by looking at all
    * files in a directory and generating a unique name.
    *
    * @note Put of all files uses overwrite semantic, i.e. if path returned by computeUrl already exists content will be overwritten.
    *       If that doesn't suit your use case use computeUrl to guard against overwriting existing files.
    *
    * @param computeUrl operation to compute the url of the first file and all subsequent files.
    * @param limit maximum size in bytes for each file.
    * @return sink of bytes
    */
  def putRotate[A](computeUrl: F[Url[A]], limit: Long): Pipe[F, Byte, Unit]

  def stat[A](url: Url[A]): Stream[F, Url[BlobType]]
}

object Store {
  type Generic[F[_]] = Store[F, FsObject]

  implicit def syntax[F[_]: Files: Concurrent, B](store: Store[F, B]): StoreOps[F, B] =
    new StoreOps[F, B](store)

  /** Validates input URLs before delegating to underlying store. This allows different stores to be exposed
    * under a the same, and wider, interface. For instance, we can expose FileStore's with Path input as a
    * BlobStore with bucket input and which validates that the bucket equals the FileStore's authority.
    *
    * Use `transformPath` to control how paths retrieved from input URLs are converted to paths for FileStores
    */
  //
  private[blobstore] class DelegatingStore[F[_]: MonadError[*[_], Throwable], Blob](
    delegate: PathStore[F, Blob],
    validateInput: Url.Plain => Validated[Throwable, Path.Plain] = (_: Url.Plain).path.valid[Throwable]
  ) extends Store[F, Blob] {
    private def plainUrl[A](url: Url[A]): Url.Plain = url.copy(path = url.path.plain)

    override def list[A](url: Url[A], recursive: Boolean): Stream[F, Url[Blob]] =
      validateInput(plainUrl(url))
        .liftTo[Stream[F, *]]
        .flatMap(delegate.list(_, recursive))
        .map(p => url.copy(path = p))

    override def get[A](url: Url[A], chunkSize: Int): Stream[F, Byte] =
      validateInput(plainUrl(url)).liftTo[Stream[F, *]].flatMap(delegate.get(_, chunkSize))

    override def put[A](url: Url[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] =
      s => validateInput(plainUrl(url)).liftTo[Stream[F, *]].flatMap(p => s.through(delegate.put(p, overwrite, size)))

    override def move[A, B](src: Url[A], dst: Url[B]): F[Unit] =
      (validateInput(plainUrl(src)).toEither, validateInput(plainUrl(dst)).toEither).tupled.liftTo[F].flatMap(
        (delegate.move[
          String,
          String
        ] _).tupled
      )

    override def copy[A, B](src: Url[A], dst: Url[B]): F[Unit] =
      (validateInput(plainUrl(src)).toEither, validateInput(plainUrl(dst)).toEither).tupled.liftTo[F].flatMap(
        (delegate.copy[
          String,
          String
        ] _).tupled
      )

    override def remove[A](url: Url[A], recursive: Boolean): F[Unit] =
      validateInput(plainUrl(url)).liftTo[F].flatMap(delegate.remove(_, recursive))

    override def stat[A](url: Url[A]): Stream[F, Url[Blob]] =
      validateInput(plainUrl(url)).liftTo[Stream[F, *]].flatMap(s =>
        Stream.eval(delegate.stat[String](s)).unNone.map(url.withPath)
      )

    override def putRotate[A](computeUrl: F[Url[A]], limit: Long): Pipe[F, Byte, Unit] = {
      val p = computeUrl.flatMap(url => validateInput(plainUrl(url)).liftTo[F])
      delegate.putRotate(p, limit)
    }
  }

}
