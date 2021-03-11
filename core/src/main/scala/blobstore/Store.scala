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

import java.nio.charset.StandardCharsets

import blobstore.url.{Authority, Path, Url}
import blobstore.url.Authority.Bucket
import blobstore.url.exception.MultipleUrlValidationException
import cats.{ApplicativeError, MonadError}
import cats.data.Validated
import cats.effect.{ContextShift, Sync}
import cats.syntax.all._
import fs2.{Pipe, Stream}

import scala.util.{Failure, Success, Try}

trait Store[F[_], A <: Authority, +BlobType] {

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
  def list(url: Url[A], recursive: Boolean = false): Stream[F, Path[BlobType]]

  /** @param url to get
    * @param chunkSize bytes to read in each chunk.
    * @return stream of bytes
    */
  def get(url: Url[A], chunkSize: Int): Stream[F, Byte]

  /** @param url to put
    * @param overwrite when true putting to path with pre-existing file would overwrite the content, otherwise – fail with error.
    * @return sink of bytes
    */
  def put(url: Url[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit]

  def put(contents: String, url: Url[A]): Stream[F, Unit] = {
    val bytes = contents.getBytes(StandardCharsets.UTF_8)
    Stream
      .emits(bytes)
      .covary[F]
      .through(put(url, size = Some(bytes.length.toLong)))
  }

  /** Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def move(src: Url[A], dst: Url[A]): F[Unit]

  /** Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def copy(src: Url[A], dst: Url[A]): F[Unit]

  /** Remove bytes for given path. Call should succeed even if there is nothing stored at that path.
    * @param url to remove
    * @return F[Unit]
    */
  def remove(url: Url[A], recursive: Boolean = false): F[Unit]

  /** Writes all data to a sequence of blobs/files, each limited in size to `limit`.
    *
    * The `computePath` operation is used to compute the path of the first file
    * and every subsequent file. Typically, the next file should be determined
    * by analyzing the current state of the filesystem -- e.g., by looking at all
    * files in a directory and generating a unique name.
    *
    * @note Put of all files uses overwrite semantic, i.e. if path returned by computePath already exists content will be overwritten.
    *       If that doesn't suit your use case use computePath to guard against overwriting existing files.
    *
    * @param computePath operation to compute the path of the first file and all subsequent files.
    * @param limit maximum size in bytes for each file.
    * @return sink of bytes
    */
  def putRotate(computePath: F[Url[A]], limit: Long): Pipe[F, Byte, Unit]

  def widen(implicit ME: MonadError[F, Throwable]): Store[F, Authority.Standard, BlobType]

  def stat(url: Url[A]): Stream[F, Path[BlobType]]
}

object Store {
  implicit def syntax[F[_]: Sync: ContextShift, A <: Authority, B](store: Store[F, A, B]): StoreOps[F, A, B] =
    new StoreOps[F, A, B](store)

  /** Blobstores operates on buckets and returns store specific blob types
    *
    * For example, S3 is a BlobStore[F, S3MetaInfo]
    */
  type BlobStore[F[_], B] = Store[F, Authority.Bucket, B]

  /** Validates input URLs before delegating to underlying store. This allows different stores to be exposed
    * under a the same, and wider, interface. For instance, we can expose FileStore's with Path input as a
    * BlobStore with bucket input and which validates that the bucket equals the FileStore's authority.
    *
    * Use `transformPath` to control how paths retrieved from input URLs are converted to paths for FileStores
    */
  //
  private[blobstore] class DelegatingStore[F[_]: MonadError[*[_], Throwable], AA <: Authority, Blob](
    underlying: Either[BlobStore[F, Blob], PathStore[F, Blob]],
    pathStoreValidate: Url[AA] => Validated[Throwable, Path.Plain] = (_: Url[AA]).path.valid[Throwable]
  ) extends Store[F, AA, Blob] {

    override def list(url: Url[AA], recursive: Boolean): Stream[F, Path[Blob]] =
      validateAndInvoke[Stream[F, *], Path[Blob]](url)(
        _.list(_, recursive),
        _.list(_, recursive)
      )

    override def get(url: Url[AA], chunkSize: Int): Stream[F, Byte] =
      validateAndInvoke[Stream[F, *], Byte](url)(
        _.get(_, chunkSize),
        _.get(_, chunkSize)
      )

    override def put(url: Url[AA], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] =
      s => {
        val t = validateAndInvoke[Try, Pipe[F, Byte, Unit]](url)(
          _.put(_, overwrite, size).pure[Try],
          _.put(_, overwrite, size).pure[Try]
        )

        t match {
          case Success(value)     => s.through(value)
          case Failure(exception) => s.flatMap(_ => Stream.raiseError[F](exception))
        }
      }

    override def move(src: Url[AA], dst: Url[AA]): F[Unit] =
      underlying match {
        case Left(blobstore) =>
          (validateForBlobStore[F](src), validateForBlobStore[F](dst)).tupled.flatMap {
            case (s, d) => blobstore.move(s, d)
          }
        case Right(filestore) =>
          (validateForFileStore[F](src), validateForFileStore[F](dst)).tupled.flatMap {
            case (s, d) => filestore.move(s, d)
          }
      }

    override def copy(src: Url[AA], dst: Url[AA]): F[Unit] =
      underlying match {
        case Left(blobstore) =>
          (validateForBlobStore[F](src), validateForBlobStore[F](dst)).tupled.flatMap {
            case (s, d) => blobstore.copy(s, d)
          }
        case Right(filestore) =>
          (validateForFileStore[F](src), validateForFileStore[F](dst)).tupled.flatMap {
            case (s, d) => filestore.copy(s, d)
          }
      }

    override def remove(url: Url[AA], recursive: Boolean): F[Unit] =
      validateAndInvoke[F, Unit](url)(
        _.remove(_, recursive),
        _.remove(_, recursive)
      )

    override def stat(url: Url[AA]): Stream[F, Path[Blob]] =
      validateAndInvoke[Stream[F, *], Path[Blob]](url)(
        _.stat(_),
        (a, b) => Stream.eval(a.stat(b)).unNone
      )

    override def putRotate(computePath: F[Url[AA]], limit: Long): Pipe[F, Byte, Unit] =
      underlying match {
        case Left(blobStore) =>
          val u = computePath.flatMap(u => validateForBlobStore[F](u))
          blobStore.putRotate(u, limit)
        case Right(fileStore) =>
          val u = computePath.flatMap(u => validateForFileStore[F](u))
          fileStore.putRotate(u, limit)
      }

    private def validateForBlobStore[G[_]: ApplicativeError[*[_], Throwable]](url: Url[AA]): G[Url[Bucket]] =
      Url.bucket(url.show).leftMap(MultipleUrlValidationException.apply).liftTo[G]

    private def validateForFileStore[G[_]: ApplicativeError[*[_], Throwable]](
      url: Url[AA]
    ): G[Path.Plain] =
      pathStoreValidate(url).liftTo[G]

    private def validateAndInvoke[G[_]: MonadError[*[_], Throwable], A](url: Url[AA])(
      f: (BlobStore[F, Blob], Url[Bucket]) => G[A],
      g: (PathStore[F, Blob], Path.Plain) => G[A]
    ): G[A] =
      underlying match {
        case Left(blobStore)  => validateForBlobStore[G](url).flatMap(f(blobStore, _))
        case Right(fileStore) => validateForFileStore[G](url).flatMap(g(fileStore, _))
      }

    override def widen(implicit ME: MonadError[F, Throwable]): Store[F, Authority.Standard, Blob] = {
      underlying match {
        case Left(blobStore)  => blobStore.widen(ME)
        case Right(pathStore) => pathStore.liftToStandard
      }
    }
  }

}
