package blobstore.experiment

import java.nio.charset.StandardCharsets

import blobstore.url.Authority.Bucket
import blobstore.url.{FsObject, Path, Url}
import cats.MonadError
import cats.effect.{Blocker, ContextShift, Sync}
import cats.syntax.all._
import fs2.{Pipe, Stream}

/**
  *
  * A store that represents a cloud provider. Each store is bound to one URL scheme.
  *
  * These stores are characterized by the user authenticating with a cloud provider prior to accessing
  * authorities, typically called buckets. This differs from for single authority stores, for instance SFTP, where
  * users authenticate with the samee authority that the store is accessing.
  *
  * @tparam F Compurational context
  * @tparam BlobType Typically a native type from the cloud vendor's client libraries that describes a blob
  */
abstract class CloudStore[F[_]: MonadError[*[_], Throwable], BlobType] {

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
  def list[A](bucketName: Bucket, path: Path[A], recursive: Boolean): Stream[F, Path[BlobType]]

  /**
    * Collect all list results in the same order as the original list Stream
    * @param path Path to list
    * @return F\[List\[Path\]\] with all items in the result
    */
  def listAll[A](bucketName: Bucket, path: Path[A], recursive: Boolean)(implicit F: Sync[F]): F[List[Path[BlobType]]] =
    list(bucketName, path, recursive).compile.toList

  /**
    * Get bytes for the given Path.
    * @param path to get
    * @param chunkSize bytes to read in each chunk.
    * @return stream of bytes
    */
  def get[A](bucketName: Bucket, path: Path[A], chunkSize: Int): Stream[F, Byte]

  /**
    * Provides a Sink that writes bytes into the provided path.
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
  def put[A](bucketName: Bucket, path: Path[A], overwrite: Boolean, size: Option[Long]): Pipe[F, Byte, Unit]

  def put(bucketName: Bucket, path: Path[BlobType], overwrite: Boolean)(implicit B: FsObject[BlobType]): Pipe[F, Byte, Unit] =
    put(bucketName, path, overwrite, Option(path.size))

  def put[A](bucketName: Bucket, contents: String, path: Path[A], overwrite: Boolean)(implicit F: Sync[F]): F[Unit] = {
    val bytes = contents.getBytes(StandardCharsets.UTF_8)
    Stream
      .emits(bytes)
      .covary[F]
      .through(put(bucketName, path, overwrite, Option(bytes.size.toLong)))
      .compile
      .drain
  }

  /**
    * Write contents of src file into dst Path
    * @param src java.nio.file.Path
    * @param dst Path to write to
    * @return F[Unit]
    */
  def put[A](bucketName: Bucket, src: java.nio.file.Path, dst: Path[A], blocker: Blocker, overwrite: Boolean)(implicit F: Sync[F], CS: ContextShift[F]): F[Unit] =
    fs2.io.file
      .readAll(src, blocker, 4096)
      .through(put(bucketName = bucketName, path = dst, overwrite = overwrite, size = Option(src.toFile.length)))
      .compile
      .drain

  def put(url: Url[Bucket], overwrite: Boolean, size: Option[Long]): Pipe[F, Byte, Unit] =
    _.through(put(url.authority, url.path, overwrite, size))

  /**
    * Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def move[A](src: (Bucket, Path[A]), dst: (Bucket, Path[A])): F[Unit]

  /**
    * Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def move(src: Url[Bucket], dst: Url[Bucket]): F[Unit] =
    move(src.authority -> src.path, dst.authority -> dst.path)

  /**
    * Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def copy[A](src: (Bucket, Path[A]), dst: (Bucket, Path[A])): F[Unit]

  /**
    * Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def copy(src: Url[Bucket], dst: Url[Bucket]): F[Unit] =
    copy(src.authority -> src.path, dst.authority -> dst.path)

  /**
    * Remove bytes for given path. Call should succeed even if there is nothing stored at that path.
    * @param path to remove
    * @return F[Unit]
    */
  def remove[A](bucketName: Bucket, path: Path[A]): F[Unit]

  /**
    * Remove bytes for given path. Call should succeed even if there is nothing stored at that path.
    * @param path to remove
    * @return F[Unit]
    */
  def remove(path: Url[Bucket]): F[Unit] =
    remove(path.authority, path.path)

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
  def putRotate[A](bucketName: Bucket, computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit]

  /**
    * get with default buffer size of 4kb
    * @param path Path to get
    * @return Stream of Byte
    */
  def get[A](bucketName: Bucket, path: Path[A]): Stream[F, Byte] = get(bucketName, path, 4096)

  /**
    * Get bytes for the given Path.
    * @param chunkSize bytes to read in each chunk.
    * @return stream of bytes
    */
  def get(url: Url[Bucket], chunkSize: Int): Stream[F, Byte] =
    get(url.authority, url.path, chunkSize)

  /**
    * getContents with default UTF8 decoder
    * @param path Path to get
    * @return F[String] with file contents
    */
  def getContents[A](bucketName: Bucket, path: Path[A])(implicit F: Sync[F]): F[String] = getContents(bucketName, path, fs2.text.utf8Decode)

  /**
    * Decode get bytes from path into a string using decoder and return concatenated string.
    *
    * USE WITH CARE, this loads all file contents into memory.
    *
    * @param path Path to get
    * @param decoder Pipe[F, Byte, String]
    * @return F[String] with file contents
    */
  def getContents[A](bucketName: Bucket, path: Path[A], decoder: Pipe[F, Byte, String])(implicit F: Sync[F]): F[String] = {
    get(bucketName, path).through(decoder).compile.toList.map(_.mkString)
  }

}
