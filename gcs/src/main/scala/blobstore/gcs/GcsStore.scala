package blobstore.gcs

import blobstore.{putRotateBase, Store, StoreOps}
import blobstore.url.{Path, Url}
import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Acl, Blob, BlobId, BlobInfo, Storage, StorageException}
import com.google.cloud.storage.Storage.{BlobGetOption, BlobListOption, BlobWriteOption, CopyRequest}
import fs2.{Chunk, Pipe, Stream}

import java.io.OutputStream
import java.nio.channels.Channels
import scala.jdk.CollectionConverters._

/** @param storage configured instance of GCS Storage
  * @param acls list of Access Control List objects to be set on all uploads.
  * @param defaultTrailingSlashFiles test if folders returned by `list` are files with trailing slashes in their names.
  *                                  This controls behaviour of `list` method from Store trait.
  *                                  Use [[listUnderlying]] to control on per-invocation basis.
  * @param defaultDirectDownload use direct download.
  *                              When enabled the whole media content is downloaded in a single request (but still streamed).
  *                              Otherwise use the resumable media download protocol to download in data chunks.
  *                              This controls behaviour of `get` method from Store trait.
  *                              Use [[getUnderlying]] to control on per-invocation basis.
  */
class GcsStore[F[_]: Async](
  storage: Storage,
  acls: List[Acl] = Nil,
  defaultTrailingSlashFiles: Boolean = false,
  defaultDirectDownload: Boolean = false
) extends Store[F, GcsBlob] {

  override def list[A](url: Url[A], recursive: Boolean = false): Stream[F, Url[GcsBlob]] =
    list(url, recursive, List.empty)

  def list[A](url: Url[A], recursive: Boolean, options: List[BlobListOption]): Stream[F, Url[GcsBlob]] =
    listUnderlying(url, defaultTrailingSlashFiles, recursive, options: _*)

  override def get[A](url: Url[A], chunkSize: Int): Stream[F, Byte] =
    get(url, chunkSize, List.empty)

  def get[A](url: Url[A], chunkSize: Int, options: List[BlobGetOption]): Stream[F, Byte] = {
    getUnderlying(url, chunkSize, defaultDirectDownload, options: _*)
  }

  override def put[A](url: Url[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(newOutputStream(url, overwrite, List.empty), closeAfterUse = true)

  def put[A](url: Url[A], overwrite: Boolean, options: List[BlobWriteOption]): Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(newOutputStream(url, overwrite, options), closeAfterUse = true)

  def put[A](path: Path[GcsBlob], options: List[BlobWriteOption]): Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(newOutputStream(path.representation.blob, options), closeAfterUse = true)

  override def remove[A](url: Url[A], recursive: Boolean = false): F[Unit] =
    if (recursive) new StoreOps[F, GcsBlob](this).removeAll(url).void
    else Async[F].blocking(storage.delete(GcsStore.toBlobId(url))).void

  override def putRotate[A](computeUrl: F[Url[A]], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, OutputStream] =
      Resource.make(computeUrl.flatMap(newOutputStream(_)))(os => Async[F].blocking(os.close()))

    putRotateBase(limit, openNewFile)(os => bytes => Async[F].blocking(os.write(bytes.toArray)))
  }

  def getUnderlying[A](
    url: Url[A],
    chunkSize: Int,
    direct: Boolean,
    options: BlobGetOption*
  ): Stream[F, Byte] =
    Stream.eval(Async[F].blocking(Option(storage.get(GcsStore.toBlobId(url), options: _*)))).flatMap {
      case None => Stream.raiseError[F](new StorageException(404, show"Object not found, ${url.copy(scheme = "gs")}"))
      case Some(blob) =>
        if (direct)
          getDirect(blob, chunkSize)
        else
          fs2.io.readInputStream(
            Channels.newInputStream {
              val reader = blob.reader()
              reader.setChunkSize(chunkSize.max(GcsStore.minimalReaderChunkSize))
              reader
            }.pure[F],
            chunkSize,
            closeAfterUse = true
          )

    }

  private def getDirect(blob: Blob, chunkSize: Int): Stream[F, Byte] =
    fs2.io.readOutputStream(chunkSize)(os => Async[F].blocking(blob.downloadTo(os)))

  def listUnderlying[A](
    url: Url[A],
    expectTrailingSlashFiles: Boolean,
    recursive: Boolean,
    inputOptions: BlobListOption*
  ): Stream[F, Url[GcsBlob]] = {
    val blobId = GcsStore.toBlobId(url)

    val options         = List(BlobListOption.prefix(if (blobId.getName == "/") "" else blobId.getName)) ++ inputOptions
    val blobListOptions = if (recursive) options else BlobListOption.currentDirectory() :: options
    Stream.unfoldChunkEval[F, () => Option[Page[Blob]], Path[Blob]] { () =>
      Some(storage.list(blobId.getBucket, blobListOptions: _*))
    } { getPage =>
      Async[F].blocking(getPage()).flatMap {
        case None => none[(Chunk[Path[Blob]], () => Option[Page[Blob]])].pure[F]
        case Some(page) =>
          page.getValues.asScala.toList
            .traverse {
              case blob if blob.isDirectory =>
                if (expectTrailingSlashFiles) Async[F].blocking(Option(storage.get(blob.getBlobId)).getOrElse(blob))
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
  }.map(p => url.copy(path = p.as(GcsBlob(p.representation))))

  private def newOutputStream[A](
    url: Url[A],
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
    Async[F].blocking(Channels.newOutputStream(storage.writer(blobInfo, options: _*)))

  /** Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    *
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  override def move[A, B](src: Url[A], dst: Url[B]): F[Unit] =
    copy(src, dst) >> remove(src, recursive = true)

  /** Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    *
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  override def copy[A, B](src: Url[A], dst: Url[B]): F[Unit] =
    Async[F].blocking(storage.copy(CopyRequest.of(GcsStore.toBlobId(src), GcsStore.toBlobId(dst))).getResult).void

  override def stat[A](url: Url[A]): Stream[F, Url[GcsBlob]] =
    Stream.eval(Async[F].blocking(Option(storage.get(GcsStore.toBlobId(url)))))
      .unNone
      .map(b => url.withPath(Path.of(b.getName, GcsBlob(b))))

}

object GcsStore {

  /** @param storage configured instance of GCS Storage
    * @param acls list of Access Control List objects to be set on all uploads.
    * @param defaultTrailingSlashFiles test if folders returned by `GcsStore.list` are files with trailing slashes in their names.
    *                                  This controls behaviour of `GcsStore.list` method from Store trait.
    *                                  Use [[GcsStore.listUnderlying]] to control on per-invocation basis.
    * @param defaultDirectDownload use direct download.
    *                              When enabled the whole media content is downloaded in a single request (but still streamed).
    *                              Otherwise use the resumable media download protocol to download in data chunks.
    *                              This controls behaviour of `GcsStore.get` method from Store trait.
    *                              Use [[GcsStore.getUnderlying]] to control on per-invocation basis.
    */
  def apply[F[_]: Async](
    storage: Storage,
    acls: List[Acl] = Nil,
    defaultTrailingSlashFiles: Boolean = false,
    defaultDirectDownload: Boolean = false
  ): GcsStore[F] =
    new GcsStore(
      storage = storage,
      acls = acls,
      defaultTrailingSlashFiles = defaultTrailingSlashFiles,
      defaultDirectDownload = defaultDirectDownload
    )

  private val minimalReaderChunkSize = 2 * 1024 * 1024 // BlobReadChannel.DEFAULT_CHUNK_SIZE

  private def toBlobId[A](url: Url[A]): BlobId =
    BlobId.of(url.authority.show, url.path.show.stripPrefix("/"))
}
