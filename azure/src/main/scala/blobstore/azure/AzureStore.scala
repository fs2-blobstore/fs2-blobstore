package blobstore
package azure

import java.nio.ByteBuffer
import java.time.Duration
import java.time.temporal.ChronoUnit

import cats.syntax.all._
import cats.instances.string._
import cats.instances.option._
import cats.effect.{ConcurrentEffect, Resource}
import com.azure.core.util.FluxUtil
import com.azure.storage.blob.BlobServiceAsyncClient
import com.azure.storage.blob.models._
import com.azure.storage.common.implementation.Constants
import fs2.{Chunk, Pipe, Stream}
import fs2.interop.reactivestreams._
import reactor.core.publisher.{Flux, Mono}
import util.liftJavaFuture

import scala.jdk.CollectionConverters._

/**
  * @param azure - Azure Blob Service Async Client
  * @param defaultFullMetadata – return full object metadata on [[list]], requires additional request per object.
  *                              Metadata returned by default: size, lastModified, eTag, storageClass.
  *                              This controls behaviour of [[list]] method from Store trait.
  *                              Use [[listUnderlying]] to control on per-invocation basis.
  * @param defaultTrailingSlashFiles - test if folders returned by [[list]] are files with trailing slashes in their names.
  *                                  This controls behaviour of [[list]] method from Store trait.
  *                                  Use [[listUnderlying]] to control on per-invocation basis.
  * @param blockSize  - For upload, The block size is the size of each block that will be staged.
  *                   This value also determines the number of requests that need to be made.
  *                   If block size is large, upload will make fewer network calls, but each individual call will send more data and will therefore take longer.
  *                   This parameter also determines the size that each buffer uses when buffering is required and consequently amount of memory consumed by such methods may be up to blockSize * numBuffers.
  * @param numBuffers - For buffered upload only, the number of buffers is the maximum number of buffers this method should allocate.
  *                   Memory will be allocated lazily as needed. Must be at least two.
  *                   Typically, the larger the number of buffers, the more parallel, and thus faster, the upload portion  of this operation will be.
  *                   The amount of memory consumed by methods using this value may be up to blockSize * numBuffers.
  */
final class AzureStore[F[_]](
  azure: BlobServiceAsyncClient,
  defaultFullMetadata: Boolean = false,
  defaultTrailingSlashFiles: Boolean = false,
  blockSize: Int = 50 * 1024 * 1024,
  numBuffers: Int = 2,
  queueSize: Int = 32
)(implicit F: ConcurrentEffect[F])
  extends Store[F] {
  require(numBuffers >= 2, "Number of buffers must be at least 2")

  override def list(path: Path, recursive: Boolean = false): Stream[F, AzurePath] =
    listUnderlying(path, defaultTrailingSlashFiles, defaultFullMetadata, recursive)

  override def get(path: Path, chunkSize: Int): Stream[F, Byte] =
    AzureStore.pathToContainerAndBlob(path) match {
      case None => Stream.raiseError(AzureStore.missingRootError(s"Unable to read '$path'"))
      case Some((container, blobName)) =>
        fromPublisher(
          azure
            .getBlobContainerAsyncClient(container)
            .getBlobAsyncClient(blobName)
            .download()
        ).flatMap(byteBuffer => Stream.chunk(Chunk.byteBuffer(byteBuffer)))
    }

  @SuppressWarnings(Array("scalafix:DisableSyntax.null"))
  override def put(path: Path, overwrite: Boolean = true): Pipe[F, Byte, Unit] =
    in =>
      AzureStore.pathToContainerAndBlob(path) match {
        case None => Stream.raiseError(AzureStore.missingRootError(s"Unable to write to '$path'"))
        case Some((container, blobName)) =>
          val blobClient = azure
            .getBlobContainerAsyncClient(container)
            .getBlobAsyncClient(blobName)
          val flux = Flux.from(in.chunks.map(_.toByteBuffer).toUnicastPublisher())
          val pto  = new ParallelTransferOptions(blockSize, numBuffers, null)
          val upload = AzurePath
            .narrow(path)
            .flatMap(AzureStore.headersMetadataAccessTier)
            .fold {
              blobClient.upload(flux, pto, overwrite)
            } {
              case (headers, meta, tier) =>
                val (overwriteCheck, requestConditions) = if (overwrite) {
                  Mono.empty -> null
                } else {
                  blobClient.exists.flatMap((exists: java.lang.Boolean) =>
                    if (exists) Mono.error[Unit](new IllegalArgumentException(Constants.BLOB_ALREADY_EXISTS))
                    else Mono.empty[Unit]
                  ) -> new BlobRequestConditions().setIfNoneMatch(Constants.HeaderConstants.ETAG_WILDCARD)
                }
                overwriteCheck
                  .`then`(blobClient.uploadWithResponse(flux, pto, headers, meta.asJava, tier, requestConditions))
                  .flatMap(resp => FluxUtil.toMono(resp))
            }

          Stream.eval(liftJavaFuture(F.delay(upload.toFuture)).void)
      }

  override def move(src: Path, dst: Path): F[Unit] =
    copy(src, dst) >> remove(src)

  @SuppressWarnings(Array("scalafix:DisableSyntax.null"))
  override def copy(src: Path, dst: Path): F[Unit] =
    for {
      (srcContainer, srcBlob) <- AzureStore
        .pathToContainerAndBlob(src)
        .fold[F[(String, String)]](AzureStore.missingRootError(s"Wrong src '$src'").raiseError)(_.pure[F])
      (dstContainer, dstBlob) <- AzureStore
        .pathToContainerAndBlob(dst)
        .fold[F[(String, String)]](AzureStore.missingRootError(s"Wrong dst '$dst'").raiseError)(_.pure[F])
      _ <- {
        val srcUrl = azure
          .getBlobContainerAsyncClient(srcContainer)
          .getBlobAsyncClient(srcBlob)
          .getBlobUrl
        val blobClient = azure
          .getBlobContainerAsyncClient(dstContainer)
          .getBlobAsyncClient(dstBlob)
        val copy = AzurePath
          .narrow(dst)
          .flatMap(AzureStore.headersMetadataAccessTier)
          .fold {
            blobClient.beginCopy(srcUrl, Duration.of(1, ChronoUnit.SECONDS))
          } {
            case (_, meta, tier) =>
              blobClient.beginCopy(
                srcUrl,
                meta.asJava,
                tier,
                RehydratePriority.STANDARD,
                null,
                null,
                Duration.of(1, ChronoUnit.SECONDS)
              )
          }
        liftJavaFuture(F.delay(copy.next().flatMap(_.getFinalResult).toFuture))
      }
    } yield ()

  override def remove(path: Path): F[Unit] =
    AzureStore.pathToContainerAndBlob(path) match {
      case None =>
        AzureStore.missingRootError(s"Unable to remove '$path'").raiseError[F, Unit]
      case Some((container, blobName)) =>
        liftJavaFuture(
          F.delay(
            azure
              .getBlobContainerAsyncClient(container)
              .getBlobAsyncClient(blobName)
              .delete()
              .toFuture
          )
        ).void.recover { case e: BlobStorageException if e.getStatusCode == 404 => () }
    }

  @SuppressWarnings(Array("scalafix:DisableSyntax.null"))
  override def putRotate(computePath: F[Path], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile = for {
      path <- Resource.liftF(computePath)
      (container, blob) <- Resource.liftF(
        AzureStore
          .pathToContainerAndBlob(path)
          .fold[F[(String, String)]](AzureStore.missingRootError(s"Unable to write to '$path'").raiseError)(F.pure)
      )
      queue <- Resource.liftF(fs2.concurrent.Queue.bounded[F, Option[ByteBuffer]](queueSize))
      blobClient = azure
        .getBlobContainerAsyncClient(container)
        .getBlobAsyncClient(blob)
      pto  = new ParallelTransferOptions(blockSize, numBuffers, null)
      flux = Flux.from(queue.dequeue.unNoneTerminate.toUnicastPublisher())
      upload = AzurePath
        .narrow(path)
        .flatMap(AzureStore.headersMetadataAccessTier)
        .fold {
          blobClient.upload(flux, pto, true)
        } {
          case (headers, meta, tier) =>
            blobClient
              .uploadWithResponse(flux, pto, headers, meta.asJava, tier, null)
              .flatMap(resp => FluxUtil.toMono(resp))
        }
      _ <- Resource.make(F.start(liftJavaFuture(F.delay(upload.toFuture)).void))(_.join)
      _ <- Resource.make(F.unit)(_ => queue.enqueue1(None))
    } yield queue
    putRotateBase(limit, openNewFile)(queue => bytes => queue.enqueue1(Some(bytes.toByteBuffer)))
  }

  def listUnderlying(
    path: Path,
    fullMetadata: Boolean,
    expectTrailingSlashFiles: Boolean,
    recursive: Boolean
  ): Stream[F, AzurePath] =
    AzureStore.pathToContainerAndBlob(path) match {
      case None => Stream.raiseError(AzureStore.missingRootError(s"Unable to list '$path'"))
      case Some((container, blobNameOrPrefix)) =>
        val options = new ListBlobsOptions()
          .setPrefix(if (blobNameOrPrefix == "/") "" else blobNameOrPrefix)
          .setDetails(new BlobListDetails().setRetrieveMetadata(fullMetadata))
        val containerClient = azure.getBlobContainerAsyncClient(container)
        val blobPagedFlux =
          if (recursive) containerClient.listBlobs(options)
          else containerClient.listBlobsByHierarchy("/", options)
        fromPublisher(blobPagedFlux)
          .filterNot(blobItem => Option(blobItem.isDeleted).contains(true))
          .flatMap { blobItem: BlobItem =>
            val properties: F[Option[(BlobItemProperties, Map[String, String])]] =
              if (Option(blobItem.isPrefix: Boolean).contains(true) && expectTrailingSlashFiles) {
                liftJavaFuture(F.delay(containerClient.getBlobAsyncClient(blobItem.getName).getProperties.toFuture))
                  .map(_.some)
                  .recover { case e: BlobStorageException if e.getStatusCode == 404 => none }
                  .map(_.map(AzureStore.toBlobItemProperties))
              } else {
                (
                  Option(blobItem.getProperties),
                  Option(blobItem.getMetadata).fold(Map.empty[String, String])(_.asScala.toMap).some
                ).tupled.pure[F]
              }
            Stream.eval(properties).map(blobItem -> _)
          }
          .map {
            case (blobItem, propsMetadata) =>
              AzurePath(
                container,
                blobItem.getName,
                propsMetadata.map(_._1),
                propsMetadata.fold(Map.empty[String, String])(_._2)
              )
          }
    }
}

object AzureStore {
  def apply[F[_]](
    azure: BlobServiceAsyncClient,
    defaultFullMetadata: Boolean = false,
    defaultTrailingSlashFiles: Boolean = false,
    blockSize: Int = 50 * 1024 * 1024,
    numBuffers: Int = 2,
    queueSize: Int = 32
  )(implicit F: ConcurrentEffect[F]): F[AzureStore[F]] =
    if (numBuffers < 2) new IllegalArgumentException(s"Number of buffers must be at least 2").raiseError
    else new AzureStore(azure, defaultFullMetadata, defaultTrailingSlashFiles, blockSize, numBuffers, queueSize).pure[F]

  private def missingRootError(msg: String) =
    new IllegalArgumentException(s"$msg - root (container) is required to reference blobs in Azure Storage")

  private def pathToContainerAndBlob(path: Path): Option[(String, String)] = AzurePath.narrow(path) match {
    case Some(azurePath) => Some(azurePath.container -> azurePath.blob)
    case None =>
      path.root.map { container =>
        val blob = path.pathFromRoot
          .mkString_("/") ++ path.fileName.fold("/")((if (path.pathFromRoot.isEmpty) "" else "/") ++ _)
        container -> blob
      }
  }

  private def toBlobItemProperties(bp: BlobProperties): (BlobItemProperties, Map[String, String]) = {
    val bip = new BlobItemProperties()
      .setAccessTier(bp.getAccessTier)
      .setAccessTierChangeTime(bp.getAccessTierChangeTime)
      .setArchiveStatus(bp.getArchiveStatus)
      .setBlobSequenceNumber(bp.getBlobSequenceNumber)
      .setBlobType(bp.getBlobType)
      .setCacheControl(bp.getCacheControl)
      .setContentDisposition(bp.getContentDisposition)
      .setContentEncoding(bp.getContentEncoding)
      .setContentLanguage(bp.getContentLanguage)
      .setContentLength(bp.getBlobSize)
      .setContentMd5(bp.getContentMd5)
      .setContentType(bp.getContentType)
      .setCopyCompletionTime(bp.getCopyCompletionTime)
      .setCopyId(bp.getCopyId)
      .setCopyProgress(bp.getCopyProgress)
      .setCopySource(bp.getCopySource)
      .setCopyStatus(bp.getCopyStatus)
      .setCopyStatusDescription(bp.getCopyStatusDescription)
      .setCreationTime(bp.getCreationTime)
      .setETag(bp.getETag)
      .setLastModified(bp.getLastModified)
      .setLeaseDuration(bp.getLeaseDuration)
      .setLeaseStatus(bp.getLeaseStatus)

    bip -> Option(bp.getMetadata).fold(Map.empty[String, String])(_.asScala.toMap)
  }

  private def headersMetadataAccessTier(path: AzurePath): Option[(BlobHttpHeaders, Map[String, String], AccessTier)] =
    path.properties.map { blobProperties =>
      val at = blobProperties.getAccessTier
      val headers = new BlobHttpHeaders()
        .setCacheControl(blobProperties.getCacheControl)
        .setContentDisposition(blobProperties.getContentDisposition)
        .setContentEncoding(blobProperties.getContentEncoding)
        .setContentLanguage(blobProperties.getContentLanguage)
        .setContentMd5(blobProperties.getContentMd5)
        .setContentType(blobProperties.getContentType)
      (headers, path.meta, at)
    }
}
