package blobstore

import java.nio.charset.StandardCharsets

import blobstore.url.{Authority, FileSystemObject, Path, Url}
import blobstore.url.general.UniversalFileSystemObject
import blobstore.Store.{BlobStore, UniversalStore}
import fs2.{Pipe, Stream}

abstract class PathStore[F[_], BlobType] {

  def authority: Authority

  /**
    * List paths.
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
  def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[BlobType]]

  /**
    * Get bytes for the given Path.
    * @param path to get
    * @param chunkSize bytes to read in each chunk.
    * @return stream of bytes
    */
  def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte]

  /**
    * Provides a Sink that writes bytes into the provided path.
    *
    * It is highly recommended to provide `Path.size` when writing as it allows for optimizations in some store.
    * Specifically, S3Store will behave very poorly if no size is provided as it will load all bytes in memory before
    * writing content to S3 server.
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

  /**
    * Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def move[A, B](src: Path[A], dst: Path[B]): F[Unit]

  /**
    * Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def copy[A, B](src: Path[A], dst: Path[B]): F[Unit]

  /**
    * Remove bytes for given path. Call should succeed even if there is nothing stored at that path.
    * @param url to remove
    * @return F[Unit]
    */
  def remove[A](url: Path[A], recursive: Boolean): F[Unit]

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
  def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit]

  /**
    * Lifts this FileStore to a Store accepting URLs with authority `A` and exposing blobs of type `B`. You must provide
    * a mapping from this Store's BlobType to B, and you may provide a function `g` for controlling input paths to this store.
    *
    * Input URLs to the returned store are validated against this Store's authority before the path is extracted and passed
    * to this store.
    */
  def liftTo[A <: Authority, B](f: BlobType => B, g: Path.Plain => Path.Plain = identity): Store[F, A, B]

  def liftToUniversal(implicit fso: FileSystemObject[BlobType]): UniversalStore[F] =
    liftTo[Authority.Standard, UniversalFileSystemObject](fso.universal)

  def liftToBlobStore: BlobStore[F, BlobType] =
    liftTo[Authority.Bucket, BlobType](identity)

  def liftTo[AA <: Authority]: Store[F, AA, BlobType] =
    liftTo[AA, BlobType](identity)

  def liftToStandard: Store[F, Authority.Standard, BlobType] =
    liftTo[Authority.Standard, BlobType](identity)

  def transferTo[A <: Authority, B, P](dstStore: Store[F, A, B], srcPath: Path[P], dstUrl: Url[A])(implicit
  fsb: FileSystemObject[B]): F[Int]

  def stat[A](path: Path[A]): F[Option[Path[BlobType]]]

  def getContents[A](path: Path[A], chunkSize: Int = 4096): F[String]
}
