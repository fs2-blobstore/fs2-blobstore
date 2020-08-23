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

import blobstore.url.Authority.{Bucket, Standard}
import blobstore.url.{Authority, Path, Url}
import fs2.{Pipe, Stream}
import blobstore.url.exception.MultipleUrlValidationException
import blobstore.url.general.UniversalFileSystemObject
import blobstore.Store.UniversalStore
import cats.{ApplicativeError, MonadError}
import cats.syntax.all._
import cats.instances.try_._

import scala.util.{Failure, Success, Try}

trait Store[F[_], A <: Authority, BlobType] {

  /**
    * List paths. See [[StoreOps.ListOps]] for convenient listAll method.
    *
    * @param path to list
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
  def list(path: Url[A], recursive: Boolean = false): Stream[F, Path[BlobType]]

  /**
    * Get bytes for the given Path. See [[StoreOps.GetOps]] for convenient get and getContents methods.
    * @param path to get
    * @param chunkSize bytes to read in each chunk.
    * @return stream of bytes
    */
  def get(path: Url[A], chunkSize: Int): Stream[F, Byte]

  /**
    * Provides a Sink that writes bytes into the provided path. See [[StoreOps.PutOps]] for convenient put String
    * and put file methods.
    *
    * It is highly recommended to provide [[Path.size]] when writing as it allows for optimizations in some store.
    * Specifically, S3Store will behave very poorly if no size is provided as it will load all bytes in memory before
    * writing content to S3 server.
    *
    * @param path to put
    * @param overwrite when true putting to path with pre-existing file would overwrite the content, otherwise – fail with error.
    * @return sink of bytes
    */
  def put(path: Url[A], overwrite: Boolean = true): Pipe[F, Byte, Unit]

  /**
    * Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def move(src: Url[A], dst: Url[A]): F[Unit]

  /**
    * Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def copy(src: Url[A], dst: Url[A]): F[Unit]

  /**
    * Remove bytes for given path. Call should succeed even if there is nothing stored at that path.
    * @param url to remove
    * @return F[Unit]
    */
  def remove(url: Url[A]): F[Unit]

  /**
    * Writes all data to a sequence of blobs/files, each limited in size to `limit`.
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

  def liftToUniversal: UniversalStore[F]
}

object Store {

  type BlobStore[F[_], B]   = Store[F, Authority.Bucket, B]
  type UniversalStore[F[_]] = Store[F, Authority.Standard, UniversalFileSystemObject]

  /**
    * Validates input URLs before delegating to underlying store
    */
  private[blobstore] class DelegatingStore[F[_]: MonadError[*[_], Throwable], Blob](
    liftBlob: Blob => UniversalFileSystemObject,
    underlying: Either[BlobStore[F, Blob], FileStore[F, Blob]]
  ) extends UniversalStore[F] {

    override def list(url: Url[Standard], recursive: Boolean): Stream[F, Path.GeneralObject] =
      biFlatMap[Stream[F, *], Path.GeneralObject](url)(
        _.list(_, recursive).map(_.map(liftBlob)),
        _.list(_, recursive).map(_.map(liftBlob))
      )

    override def get(url: Url[Standard], chunkSize: Int): Stream[F, Byte] =
      biFlatMap[Stream[F, *], Byte](url)(
        _.get(_, chunkSize),
        _.get(_, chunkSize)
      )

    override def put(url: Url[Standard], overwrite: Boolean): Pipe[F, Byte, Unit] = s => {
      val t = biFlatMap[Try, Pipe[F, Byte, Unit]](url)(
        _.put(_, overwrite).pure[Try],
        _.put(_, overwrite).pure[Try]
      )

      t match {
        case Success(value)     => s.through(value)
        case Failure(exception) => s.flatMap(_ => Stream.raiseError[F](exception))
      }
    }

    override def move(src: Url[Standard], dst: Url[Standard]): F[Unit] =
      underlying match {
        case Left(blobstore) => (validateForBlobStore[F](src), validateForBlobStore[F](dst)).tupled.flatMap {
            case (s, d) => blobstore.move(s, d)
          }
        case Right(filestore) => (validateForFileStore[F](src, filestore), validateForFileStore[F](dst, filestore)).tupled.flatMap {
            case (s, d) => filestore.move(s, d)
          }
      }

    override def copy(src: Url[Standard], dst: Url[Standard]): F[Unit] =
      underlying match {
        case Left(blobstore) => (validateForBlobStore[F](src), validateForBlobStore[F](dst)).tupled.flatMap {
            case (s, d) => blobstore.copy(s, d)
          }
        case Right(filestore) => (validateForFileStore[F](src, filestore), validateForFileStore[F](dst, filestore)).tupled.flatMap {
            case (s, d) => filestore.copy(s, d)
          }
      }

    override def remove(path: Url[Standard]): F[Unit] =
      biFlatMap[F, Unit](path)(
        _.remove(_),
        _.remove(_)
      )

    override def putRotate(computePath: F[Url[Standard]], limit: Long): Pipe[F, Byte, Unit] = ??? // TODO

    override def liftToUniversal: Store[F, Standard, UniversalFileSystemObject] = this

    private def validateForBlobStore[G[_]: ApplicativeError[*[_], Throwable]](url: Url.Plain): G[Url[Bucket]] =
      Url.forBucket(url.show).leftMap(MultipleUrlValidationException.apply).liftTo[G]

    private def validateForFileStore[G[_]: ApplicativeError[*[_], Throwable]](
      url: Url.Plain,
      fileStore: FileStore[F, Blob]
    ): G[Path.Plain] =
      if (url.authority.equalsIgnoreUserInfo(fileStore.authority)) url.path.pure[G]
      else
        new Exception(
          show"Expected authorities to match, but got ${url.authority} for ${fileStore.authority}"
        ).raiseError[G, Path.Plain]

    private def biFlatMap[G[_]: MonadError[*[_], Throwable], A](url: Url.Plain)(
      f: (BlobStore[F, Blob], Url[Bucket]) => G[A],
      g: (FileStore[F, Blob], Path.Plain) => G[A]
    ): G[A] =
      underlying match {
        case Left(blobStore)  => validateForBlobStore[G](url).flatMap(f(blobStore, _))
        case Right(fileStore) => validateForFileStore[G](url, fileStore).flatMap(g(fileStore, _))
      }

  }

}
