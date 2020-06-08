package blobstore
package gcs

import java.io.OutputStream
import java.nio.channels.Channels

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource}
import cats.syntax.all._
import cats.instances.string._
import cats.instances.list._
import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Acl, Blob, BlobId, BlobInfo, Storage, StorageException}
import com.google.cloud.storage.Storage.{BlobListOption, BlobWriteOption, CopyRequest}
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
final class GcsStore[F[_]](
  storage: Storage,
  blocker: Blocker,
  acls: List[Acl] = Nil,
  defaultTrailingSlashFiles: Boolean = false,
  defaultDirectDownload: Boolean = false,
  defaultMaxChunksInFlight: Option[Int] = None
)(
  implicit F: ConcurrentEffect[F],
  CS: ContextShift[F]
) extends Store[F] {

  override def list(path: Path, recursive: Boolean = false): Stream[F, GcsPath] =
    listUnderlying(path, defaultTrailingSlashFiles, recursive)

  override def get(path: Path, chunkSize: Int): Stream[F, Byte] =
    getUnderlying(path, chunkSize, direct = defaultDirectDownload, maxChunkInFlight = defaultMaxChunksInFlight)

  override def put(path: Path, overwrite: Boolean = true): Pipe[F, Byte, Unit] =
    fs2.io.writeOutputStream(newOutputStream(path, overwrite), blocker, closeAfterUse = true)

  override def move(src: Path, dst: Path): F[Unit] =
    copy(src, dst) >> remove(src)

  override def copy(src: Path, dst: Path): F[Unit] =
    for {
      srcBlobId <- GcsStore
        .pathToBlobId(src)
        .fold[F[BlobId]](GcsStore.missingRootError(s"Wrong src '$src'").raiseError)(_.pure[F])
      dstBlobId <- GcsStore
        .pathToBlobId(dst)
        .fold[F[BlobId]](GcsStore.missingRootError(s"Wrong dst '$dst'").raiseError)(_.pure[F])
      _ <- blocker.delay(storage.copy(CopyRequest.of(srcBlobId, dstBlobId)).getResult)
    } yield ()

  override def remove(path: Path): F[Unit] =
    GcsStore.pathToBlobId(path) match {
      case Some(blobId) => blocker.delay(storage.delete(blobId)).void
      case None         => GcsStore.missingRootError(s"Unable to remove '$path'").raiseError[F, Unit]
    }

  override def putRotate(computePath: F[Path], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, OutputStream] =
      Resource.make(computePath.flatMap(p => newOutputStream(p)))(os => blocker.delay(os.close()))

    putRotateBase(limit, openNewFile)(os => bytes => blocker.delay(os.write(bytes.toArray)))
  }

  def getUnderlying(path: Path, chunkSize: Int, direct: Boolean, maxChunkInFlight: Option[Int]): Stream[F, Byte] =
    GcsStore.pathToBlobId(path) match {
      case None => Stream.raiseError(GcsStore.missingRootError(s"Unable to read '$path'"))
      case Some(blobId) =>
        Stream.eval(blocker.delay(Option(storage.get(blobId)))).flatMap {
          case None => Stream.raiseError[F](new StorageException(404, s"Object not found, $path"))
          case Some(blob) =>
            if (direct) {
              getDirect(blob, chunkSize, maxChunkInFlight)
            } else {
              fs2.io.readInputStream(
                Channels.newInputStream {
                  val reader = blob.reader()
                  reader.setChunkSize(chunkSize)
                  reader
                }.pure[F],
                chunkSize,
                blocker,
                closeAfterUse = true
              )
            }
        }
    }

  private def getDirect(blob: Blob, chunkSize: Int, maxChunkInFlight: Option[Int]): Stream[F, Byte] =
    Stream.eval(Fs2OutputStream[F](chunkSize, maxChunkInFlight)).flatMap { os =>
      os.stream.concurrently(
        Stream.eval(
          F.guarantee(blocker.delay(blob.downloadTo(os)))(F.delay(os.close()))
        )
      )
    }

  def listUnderlying(path: Path, expectTrailingSlashFiles: Boolean, recursive: Boolean): Stream[F, GcsPath] =
    GcsStore.pathToBlobId(path) match {
      case None => Stream.raiseError(GcsStore.missingRootError(s"Unable to list '$path'"))
      case Some(blobId) =>
        val options         = List(BlobListOption.prefix(if (blobId.getName == "/") "" else blobId.getName))
        val blobListOptions = if (recursive) options else BlobListOption.currentDirectory() :: options
        Stream.unfoldChunkEval[F, () => Option[Page[Blob]], GcsPath] { () =>
          Some(storage.list(blobId.getBucket, blobListOptions: _*))
        } { getPage =>
          blocker.delay(getPage()).flatMap {
            case None => none[(Chunk[GcsPath], () => Option[Page[Blob]])].pure[F]
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
                    Chunk.seq(paths.map(blob => new GcsPath((blob: BlobInfo).toBuilder.build()))),
                    () => if (page.hasNextPage) Some(page.getNextPage) else None
                  ).some
                }
          }
        }
    }

  private def newOutputStream(path: Path, overwrite: Boolean = true): F[OutputStream] =
    GcsStore.pathToBlobId(path) match {
      case None => F.raiseError(GcsStore.missingRootError(s"Unable to write to '$path'"))
      case Some(blobId) =>
        F.delay {
          val blobInfo = GcsPath
            .narrow(path)
            .fold {
              val b = BlobInfo.newBuilder(blobId)
              (if (acls.nonEmpty) b.setAcl(acls.asJava) else b).build()
            }(_.blobInfo)
          val options = if (overwrite) Nil else List(BlobWriteOption.doesNotExist())
          val writer  = storage.writer(blobInfo, options: _*)
          Channels.newOutputStream(writer)
        }
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

  private def missingRootError(msg: String) =
    new IllegalArgumentException(s"$msg - root (bucket) is required to reference blobs in GCS")

  private def pathToBlobId(path: Path): Option[BlobId] =
    GcsPath
      .narrow(path)
      .fold {
        path.root.map(bucket =>
          BlobId.of(
            bucket,
            path.pathFromRoot
              .mkString_("/") ++ path.fileName.fold("/")((if (path.pathFromRoot.isEmpty) "" else "/") ++ _)
          )
        )
      } { gcsPath => Option(gcsPath.blobInfo.getBlobId) }
}
