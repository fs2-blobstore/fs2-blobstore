package blobstore

import java.nio.charset.StandardCharsets

import blobstore.url.{Authority, FsObject, Path, Url}
import blobstore.Store.BlobStore
import cats.data.Validated
import cats.syntax.all._
import fs2.{Pipe, Stream}

abstract class PathStore[F[_], BlobType] {

  /** List paths.
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
  def list[A](path: Path[A], recursive: Boolean): Stream[F, Path[BlobType]]

  /** Get bytes for the given Path.
    * @param path to get
    * @param chunkSize bytes to read in each chunk.
    * @return stream of bytes
    */
  def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte]

  /** Provides a Sink that writes bytes into the provided path.
    *
    * @param path to put
    * @param overwrite when true putting to path with pre-existing file would overwrite the content, otherwise – fail with error.
    * @return sink of bytes
    */
  def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit]

  def put[A](contents: String, path: Path[A], overwrite: Boolean): Stream[F, Unit] = {
    val bytes = contents.getBytes(StandardCharsets.UTF_8)
    Stream
      .emits(bytes)
      .covary[F]
      .through(put(path.plain, overwrite, Option(bytes.size.toLong)))
  }

  /** Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def move[A, B](src: Path[A], dst: Path[B]): F[Unit]

  /** Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def copy[A, B](src: Path[A], dst: Path[B]): F[Unit]

  /** Remove bytes for given path. Call should succeed even if there is nothing stored at that path.
    * @param url to remove
    * @return F[Unit]
    */
  def remove[A](url: Path[A], recursive: Boolean = false): F[Unit]

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
  def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit]

  /** Lifts this FileStore to a Store accepting URLs with authority `A` and exposing blobs of type `B`. You must provide
    * a mapping from this Store's BlobType to B, and you may provide a function `g` for controlling input paths to this store.
    *
    * Input URLs to the returned store are validated against this Store's authority before the path is extracted and passed
    * to this store.
    */
  def liftTo[A <: Authority](g: Url[A] => Validated[Throwable, Path.Plain]): Store[F, A, BlobType]

  def liftToBlobStore: BlobStore[F, BlobType] =
    liftTo[Authority.Bucket]((u: Url[Authority.Bucket]) => u.path.valid)

  def liftTo[AA <: Authority]: Store[F, AA, BlobType] =
    liftTo[AA]((u: Url[AA]) => u.path.valid)

  def liftToStandard: Store[F, Authority.Standard, BlobType] =
    liftTo[Authority.Standard]((u: Url[Authority.Standard]) => u.path.valid)

  def transferTo[A <: Authority, B, P](dstStore: Store[F, A, B], srcPath: Path[P], dstUrl: Url[A])(implicit
  ev: B <:< FsObject): F[Int]

  def stat[A](path: Path[A]): F[Option[Path[BlobType]]]

  def getContents[A](path: Path[A], chunkSize: Int = 4096): F[String]
}
