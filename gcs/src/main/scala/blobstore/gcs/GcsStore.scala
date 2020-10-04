package blobstore.gcs

import java.io.OutputStream
import java.nio.channels.Channels

import _root_.cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync}
import _root_.cats.instances.list._
import _root_.cats.syntax.all._
import blobstore.{putRotateBase, Store}
import blobstore.url.{Authority, FileSystemObject, Path, Url}
import blobstore.url.Authority.Bucket
import blobstore.Store.{BlobStore, UniversalStore}
import blobstore.gcs.GcsStore.toBlobId
import blobstore.url.general.UniversalFileSystemObject
import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Acl, Blob, BlobId, BlobInfo, Storage, StorageException}
import com.google.cloud.storage.Storage.{BlobGetOption, BlobListOption, BlobWriteOption, CopyRequest}
import fs2.{Chunk, Pipe, Stream}

import scala.jdk.CollectionConverters._

/**
  * @param storage configured instance of GCS Storage
  * @param blocker cats-effect Blocker to run blocking operations on.
  * @param acls list of Access Control List objects to be set on all uploads.
  * @param defaultTrailingSlashFiles test if folders returned by [[list]] are files with trailing slashes in their names.
  *                                  This controls behaviour of [[list]] method from Store trait.
  *                                  Use [[listUnderlying]] to control on per-invocation basis.
  * @param defaultDirectDownload use direct download.
  *                              When enabled the whole media content is downloaded in a single request (but still streamed).
  *                              Otherwise use the resumable media download protocol to download in data chunks.
  *                              This controls behaviour of [[get]] method from Store trait.
  *                              Use [[getUnderlying]] to control on per-invocation basis.
  * @param defaultMaxChunksInFlight limit maximum number of chunks buffered when direct download is used. Does not affect resumable download.
  *                                 This controls behaviour of [[get]] method from Store trait.
  *                                 Use [[getUnderlying]] to control on per-invocation basis.
  */
final class GcsStore[F[_]: ConcurrentEffect: ContextShift](
  storage: Storage,
  blocker: Blocker,
  acls: List[Acl] = Nil,
  defaultTrailingSlashFiles: Boolean = false,
  defaultDirectDownload: Boolean = false,
  defaultMaxChunksInFlight: Option[Int] = None
) extends BlobStore[F, GcsBlob] {

  override def list(url: Url[Bucket], recursive: Boolean = false): Stream[F, Path[GcsBlob]] =
    list(url, recursive, List.empty)

  def list(url: Url[Bucket], recursive: Boolean, options: List[BlobListOption]): Stream[F, Path[GcsBlob]] =
    listUnderlying(url, defaultTrailingSlashFiles, recursive, options: _*)

  override def get(url: Url[Bucket], chunkSize: Int): Stream[F, Byte] =
    get(url, chunkSize, List.empty)

  def get(url: Url[Bucket], chunkSize: Int, options: List[BlobGetOption]): Stream[F, Byte] = {
    getUnderlying(url, chunkSize, defaultDirectDownload, defaultMaxChunksInFlight, options: _*)
  }

  override def put(url: Url[Bucket], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(newOutputStream(url, overwrite, List.empty), blocker, closeAfterUse = true)

  def put(url: Url[Bucket], overwrite: Boolean, options: List[BlobWriteOption]): Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(newOutputStream(url, overwrite, options), blocker, closeAfterUse = true)

  def put(path: Path[GcsBlob], options: List[BlobWriteOption]): Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(newOutputStream(path.representation.blob, options), blocker, closeAfterUse = true)

  override def remove(url: Url[Bucket], recursive: Boolean): F[Unit] =
    if (recursive) removeAll(url).void
    else
      blocker.delay(storage.delete(GcsStore.toBlobId(url))).void

  override def putRotate(computePath: F[Url[Bucket]], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, OutputStream] =
      Resource.make(computePath.flatMap(newOutputStream(_)))(os => blocker.delay(os.close()))

    putRotateBase(limit, openNewFile)(os => bytes => blocker.delay(os.write(bytes.toArray)))
  }

  def getUnderlying[A](
    url: Url[Bucket],
    chunkSize: Int,
    direct: Boolean,
    maxChunksInFlight: Option[Int],
    options: BlobGetOption*
  ): Stream[F, Byte] =
    Stream.eval(blocker.delay(Option(storage.get(GcsStore.toBlobId(url), options: _*)))).flatMap {
      case None => Stream.raiseError[F](new StorageException(404, show"Object not found, ${url.copy(scheme = "gs")}"))
      case Some(blob) =>
        if (direct)
          getDirect(blob, chunkSize, maxChunksInFlight)
        else
          fs2.io.readInputStream(
            Channels.newInputStream {
              val reader = blob.reader()
              reader.setChunkSize(chunkSize.max(GcsStore.minimalReaderChunkSize))
              reader
            }.pure[F],
            chunkSize,
            blocker,
            closeAfterUse = true
          )

    }

  private def getDirect(blob: Blob, chunkSize: Int, maxChunksInFlight: Option[Int]): Stream[F, Byte] =
    Stream.eval(Fs2OutputStream[F](chunkSize, maxChunksInFlight)).flatMap { os =>
      os.stream.concurrently(
        Stream.eval(
          ConcurrentEffect[F].guarantee(blocker.delay(blob.downloadTo(os)))(Sync[F].delay(os.close()))
        )
      )
    }

  def listUnderlying[A](
    url: Url[Bucket],
    expectTrailingSlashFiles: Boolean,
    recursive: Boolean,
    inputOptions: BlobListOption*
  ): Stream[F, Path[GcsBlob]] = {
    val blobId = GcsStore.toBlobId(url)

    val options         = List(BlobListOption.prefix(if (blobId.getName == "/") "" else blobId.getName)) ++ inputOptions
    val blobListOptions = if (recursive) options else BlobListOption.currentDirectory() :: options
    Stream.unfoldChunkEval[F, () => Option[Page[Blob]], Path[Blob]] { () =>
      Some(storage.list(blobId.getBucket, blobListOptions: _*))
    } { getPage =>
      blocker.delay(getPage()).flatMap {
        case None => none[(Chunk[Path[Blob]], () => Option[Page[Blob]])].pure[F]
        case Some(page) =>
          page.getValues.asScala.toList
            .traverse {
              case blob if blob.isDirectory =>
                if (expectTrailingSlashFiles) blocker.delay(Option(storage.get(blob.getBlobId)).getOrElse(blob))
                else blob.pure[F]
              case blob =>
                blob.pure[F]
            }
            .map { paths =>
              (
                Chunk.seq(paths.map(blob => Path(blob.getName).as(blob))),
                () => if (page.hasNextPage) Some(page.getNextPage) else None
              ).some
            }
      }
    }
  }.map(_.map(GcsBlob.apply))

  private def newOutputStream[A](
    url: Url[Bucket],
    overwrite: Boolean = true,
    options: List[BlobWriteOption] = List.empty
  ): F[OutputStream] = {
    val blobId   = GcsStore.toBlobId(url)
    val builder  = BlobInfo.newBuilder(blobId)
    val blobInfo = (if (acls.nonEmpty) builder.setAcl(acls.asJava) else builder).build()

    val opts = if (overwrite) options else options ++ List(BlobWriteOption.doesNotExist())

    newOutputStream(blobInfo, opts)
  }

  private def newOutputStream[A](blobInfo: BlobInfo, options: List[BlobWriteOption]): F[OutputStream] =
    Sync[F].delay(Channels.newOutputStream(storage.writer(blobInfo, options: _*)))

  /**
    * Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    *
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  override def move(src: Url[Bucket], dst: Url[Bucket]): F[Unit] =
    copy(src, dst) >> remove(src, recursive = true)

  /**
    * Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    *
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  override def copy(src: Url[Bucket], dst: Url[Bucket]): F[Unit] =
    blocker.delay(storage.copy(CopyRequest.of(GcsStore.toBlobId(src), GcsStore.toBlobId(dst))).getResult).void

  override def stat(url: Url[Bucket]): F[Option[Path[GcsBlob]]] =
    blocker.delay(Option(storage.get(toBlobId(url))))
      .map(b => b.map(b => Path.of(b.getName, GcsBlob(b))))

  override def liftToUniversal: UniversalStore[F] =
    new Store.DelegatingStore[F, GcsBlob, Authority.Standard, UniversalFileSystemObject](
      FileSystemObject[GcsBlob].universal,
      Left(this)
    )
}

object GcsStore {
  def apply[F[_]](
    storage: Storage,
    blocker: Blocker,
    acls: List[Acl] = Nil,
    defaultTrailingSlashFiles: Boolean = false,
    defaultDirectDownload: Boolean = false,
    defaultMaxChunksInFlight: Option[Int] = None
  )(implicit F: ConcurrentEffect[F], CS: ContextShift[F]): GcsStore[F] =
    new GcsStore(
      storage = storage,
      blocker = blocker,
      acls = acls,
      defaultTrailingSlashFiles = defaultTrailingSlashFiles,
      defaultDirectDownload = defaultDirectDownload,
      defaultMaxChunksInFlight = defaultMaxChunksInFlight
    )

  private val minimalReaderChunkSize = 2 * 1024 * 1024 // BlobReadChannel.DEFAULT_CHUNK_SIZE

  private def toBlobId[A](url: Url[Bucket]): BlobId =
    BlobId.of(url.authority.show, url.path.show.stripPrefix("/"))
}
