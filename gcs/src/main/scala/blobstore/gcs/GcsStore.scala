package blobstore
package gcs

import java.nio.channels.Channels

import cats.effect.{Blocker, ContextShift, Sync}
import cats.syntax.all._
import cats.instances.string._
import cats.instances.list._
import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Acl, Blob, BlobId, BlobInfo, Storage, StorageException}
import com.google.cloud.storage.Storage.{BlobListOption, CopyRequest}
import fs2.{Chunk, Pipe, Stream}

import scala.jdk.CollectionConverters._

final class GcsStore[F[_]](
  storage: Storage,
  blocker: Blocker,
  acls: List[Acl] = Nil,
  defaultTrailingSlashFiles: Boolean = false
)(
  implicit F: Sync[F],
  CS: ContextShift[F]
) extends Store[F] {

  override def list(path: Path): Stream[F, Path] = listUnderlying(path, defaultTrailingSlashFiles)

  override def get(path: Path, chunkSize: Int): Stream[F, Byte] =
    GcsStore.pathToBlobId(path) match {
      case None => Stream.raiseError(GcsStore.missingRootError(s"Unable to read '$path'"))
      case Some(blobId) =>
        val fis = blocker.delay {
          Option(storage.get(blobId)).map(blob => Channels.newInputStream(blob.reader()))
        }
        Stream.eval(fis).flatMap {
          case Some(is) => fs2.io.readInputStream(is.pure[F], chunkSize, blocker, closeAfterUse = true)
          case None     => Stream.raiseError[F](new StorageException(404, s"Object not found, $path"))
        }
    }

  override def put(path: Path): Pipe[F, Byte, Unit] =
    GcsStore.pathToBlobId(path) match {
      case None => _ => Stream.raiseError(GcsStore.missingRootError(s"Unable to write to '$path'"))
      case Some(blobId) =>
        val fos = F.delay {
          val blobInfo = GcsPath
            .narrow(path)
            .fold {
              val b = BlobInfo.newBuilder(blobId)
              (if (acls.nonEmpty) b.setAcl(acls.asJava) else b).build()
            }(_.blobInfo)
          val writer = storage.writer(blobInfo)
          Channels.newOutputStream(writer)
        }
        fs2.io.writeOutputStream(fos, blocker, closeAfterUse = true)
    }

  override def move(src: Path, dst: Path): F[Unit] =
    copy(src, dst) *> remove(src)

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

  def listUnderlying(path: Path, expectTrailingSlashFiles: Boolean): Stream[F, GcsPath] =
    GcsStore.pathToBlobId(path) match {
      case None => Stream.raiseError(GcsStore.missingRootError(s"Unable to list '$path'"))
      case Some(blobId) =>
        Stream.unfoldChunkEval[F, () => Option[Page[Blob]], GcsPath] { () =>
          Some(
            storage.list(
              blobId.getBucket,
              BlobListOption.currentDirectory(),
              BlobListOption.prefix(
                if (blobId.getName == "/") ""
                else blobId.getName
              )
            )
          )
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
}

object GcsStore {
  def apply[F[_]](
    storage: Storage,
    blocker: Blocker,
    acls: List[Acl]
  )(implicit F: Sync[F], CS: ContextShift[F]): GcsStore[F] = new GcsStore(storage, blocker, acls)

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
