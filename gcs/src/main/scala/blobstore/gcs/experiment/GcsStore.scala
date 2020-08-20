package blobstore.gcs.experiment

import java.io.OutputStream
import java.nio.channels.Channels

import _root_.cats.data.Chain
import _root_.cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync}
import _root_.cats.instances.list._
import _root_.cats.instances.string._
import _root_.cats.syntax.all._
import blobstore.experiment.CloudStore
import blobstore.url.Authority.Bucket
import blobstore.url.Path.AbsolutePath
import blobstore.gcs.Fs2OutputStream
import blobstore.putRotateBase
import blobstore.url.Path
import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Acl, BlobId, BlobInfo, Storage, StorageException, Blob => GcsBlob}
import com.google.cloud.storage.Storage.{BlobGetOption, BlobListOption, BlobWriteOption, CopyRequest}
import fs2.{Chunk, Pipe, Stream}

import scala.jdk.CollectionConverters._

final class GcsStore[F[_]: ConcurrentEffect: ContextShift](
  storage: Storage,
  blocker: Blocker,
  acls: List[Acl] = Nil,
  defaultTrailingSlashFiles: Boolean = false,
  defaultDirectDownload: Boolean = false,
  defaultMaxChunksInFlight: Option[Int] = None
) extends CloudStore[F, GcsBlob] {

  override def list[A](bucketName: Bucket, path: Path[A], recursive: Boolean = false): Stream[F, Path[GcsBlob]] =
    listWithOptions(bucketName, path, recursive)

  def listWithOptions[A](bucketName: Bucket, path: Path[A], recursive: Boolean, options: BlobListOption*): Stream[F, Path[GcsBlob]] =
    listUnderlying(bucketName, path, defaultTrailingSlashFiles, recursive, options: _*)

  override def get[A](bucketName: Bucket, path: Path[A], chunkSize: Int): Stream[F, Byte] =
    getBlob(bucketName, path, chunkSize)

  def getBlob[A](bucketName: Bucket, path: Path[A], chunkSize: Int, options: BlobGetOption*): Stream[F, Byte] = {
    getUnderlying(bucketName, path, chunkSize, defaultDirectDownload, defaultMaxChunksInFlight, options: _*)
  }

  def put[A](bucketName: Bucket, path: Path[A], overwrite: Boolean = true, size: Option[Long]): Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(newOutputStream(bucketName, path, overwrite), blocker, closeAfterUse = true)

  override def remove[A](bucketName: Bucket, path: Path[A]): F[Unit] =
      blocker.delay(storage.delete(GcsStore.toBlobId(bucketName, path))).void

  override def putRotate[A](bucketName: Bucket, computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, OutputStream] =
      Resource.make(computePath.flatMap(p => newOutputStream(bucketName, p)))(os => blocker.delay(os.close()))

    putRotateBase(limit, openNewFile)(os => bytes => blocker.delay(os.write(bytes.toArray)))
  }

  def getUnderlying[A](bucketName: Bucket, path: Path[A], chunkSize: Int, direct: Boolean, maxChunksInFlight: Option[Int], options: BlobGetOption*): Stream[F, Byte] =
    Stream.eval(blocker.delay(Option(storage.get(GcsStore.toBlobId(bucketName, path), options: _*)))).flatMap {
      case None => Stream.raiseError[F](new StorageException(404, show"Object not found, gs://$bucketName/${path.show.stripPrefix("/")}"))
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

  private def getDirect(blob: GcsBlob, chunkSize: Int, maxChunksInFlight: Option[Int]): Stream[F, Byte] =
    Stream.eval(Fs2OutputStream[F](chunkSize, maxChunksInFlight)).flatMap { os =>
      os.stream.concurrently(
        Stream.eval(
          ConcurrentEffect[F].guarantee(blocker.delay(blob.downloadTo(os)))(Sync[F].delay(os.close()))
        )
      )
    }

  def listUnderlying[A](bucketName: Bucket, path: Path[A], expectTrailingSlashFiles: Boolean, recursive: Boolean, inputOptions: BlobListOption*): Stream[F, Path[GcsBlob]] = {
    val blobId = GcsStore.toBlobId(bucketName, path)

    val options = List(BlobListOption.prefix(if (blobId.getName == "/") "" else blobId.getName)) ++ inputOptions
    val blobListOptions = if (recursive) options else BlobListOption.currentDirectory() :: options
    Stream.unfoldChunkEval[F, () => Option[Page[GcsBlob]], Path[GcsBlob]] { () =>
      Some(storage.list(blobId.getBucket, blobListOptions: _*))
    } { getPage =>
      blocker.delay(getPage()).flatMap {
        case None => none[(Chunk[Path[GcsBlob]], () => Option[Page[GcsBlob]])].pure[F]
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
                Chunk.seq(paths.map(blob => AbsolutePath(blob, Chain(blob.getName.split("/").toIndexedSeq: _ *)))),
                () => if (page.hasNextPage) Some(page.getNextPage) else None
                ).some
            }
      }
    }
  }

  private def newOutputStream[A](bucket: Bucket, path: Path[A], overwrite: Boolean = true): F[OutputStream] = {
    val blobId = GcsStore.toBlobId(bucket, path)
    val builder = BlobInfo.newBuilder(blobId)
    val blobInfo = (if (acls.nonEmpty) builder.setAcl(acls.asJava) else builder).build()

    val options = if (overwrite) Nil else List(BlobWriteOption.doesNotExist())
    Sync[F].delay(Channels.newOutputStream(storage.writer(blobInfo, options: _*)))
  }

  /**
    * Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    *
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  override def move[A](src: (Bucket, Path[A]), dst: (Bucket, Path[A])): F[Unit] =
    copy(src, dst) >> remove[A](src._1, src._2)
  /**
    * Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    *
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def copy[A](src: (Bucket, Path[A]), dst: (Bucket, Path[A])): F[Unit] = {
    val toBlobId = (GcsStore.toBlobId[A] _ ).tupled
    blocker.delay(storage.copy(CopyRequest.of(toBlobId(src), toBlobId(dst))).getResult).void
  }
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

  private def toBlobId[A](bucketName: Bucket, path: Path[A]): BlobId =
    BlobId.of(bucketName.name.show, path.show)
}
