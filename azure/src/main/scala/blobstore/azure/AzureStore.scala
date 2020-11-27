package blobstore
package azure

import java.nio.ByteBuffer
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.function.{Function => JavaFunction}
import blobstore.Store.{BlobStore, UniversalStore}
import blobstore.url.general.UniversalFileSystemObject
import blobstore.url.{Authority, FileSystemObject, Path, Url}
import cats.syntax.all._
import cats.effect.{ConcurrentEffect, ContextShift, Resource}
import com.azure.core.util.FluxUtil
import com.azure.storage.blob.{BlobContainerAsyncClient, BlobServiceAsyncClient}
import com.azure.storage.blob.models.{BlobItemProperties, _}
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
class AzureStore[F[_]](
  azure: BlobServiceAsyncClient,
  defaultFullMetadata: Boolean = false,
  defaultTrailingSlashFiles: Boolean = false,
  blockSize: Int = 50 * 1024 * 1024,
  numBuffers: Int = 2,
  queueSize: Int = 32
)(implicit F: ConcurrentEffect[F], cs: ContextShift[F])
  extends BlobStore[F, AzureBlob] {
  require(numBuffers >= 2, "Number of buffers must be at least 2")

  override def list(url: Url[Authority.Bucket], recursive: Boolean): Stream[F, Path[AzureBlob]] =
    listUnderlying(url, defaultFullMetadata, defaultTrailingSlashFiles, recursive)

  override def get(url: Url[Authority.Bucket], chunkSize: Int): Stream[F, Byte] = {
    val (container, blobName) = AzureStore.urlToContainerAndBlob(url)
    fromPublisher(azure.getBlobContainerAsyncClient(container).getBlobAsyncClient(blobName).download())
      .flatMap(byteBuffer => Stream.chunk(Chunk.byteBuffer(byteBuffer)))
  }

  override def put(
    url: Url[Authority.Bucket],
    overwrite: Boolean = true,
    size: Option[Long] = None
  ): Pipe[F, Byte, Unit] =
    put(
      url,
      overwrite,
      size.fold(none[BlobItemProperties])(s => new BlobItemProperties().setContentLength(s).some),
      Map.empty
    )

  def put(
    url: Url[Authority.Bucket],
    overwrite: Boolean,
    properties: Option[BlobItemProperties],
    meta: Map[String, String]
  ): Pipe[F, Byte, Unit] = in => {
    val (container, blobName) = AzureStore.urlToContainerAndBlob(url)
    val blobClient = azure
      .getBlobContainerAsyncClient(container)
      .getBlobAsyncClient(blobName)
    val flux = Flux.from(in.chunks.map(_.toByteBuffer).toUnicastPublisher())
    val pto  = new ParallelTransferOptions(blockSize, numBuffers, null) // scalafix:ok
    val (overwriteCheck, requestConditions) = if (overwrite) {
      Mono.empty -> null // scalafix:ok
    } else {
      blobClient.exists.flatMap((exists: java.lang.Boolean) =>
        if (exists) Mono.error[Unit](new IllegalArgumentException(Constants.BLOB_ALREADY_EXISTS))
        else Mono.empty[Unit]
      ) -> new BlobRequestConditions().setIfNoneMatch(Constants.HeaderConstants.ETAG_WILDCARD)
    }

    val headers = properties.map(AzureStore.toHeaders)

    val upload = overwriteCheck
      .`then`(blobClient.uploadWithResponse(
        flux,
        pto,
        headers.orNull,
        meta.asJava,
        properties.flatMap(bip => Option(bip.getAccessTier)).orNull,
        requestConditions
      ))
      .flatMap(resp => FluxUtil.toMono(resp))
    Stream.eval(liftJavaFuture(F.delay(upload.toFuture)).void)
  }

  override def move(src: Url[Authority.Bucket], dst: Url[Authority.Bucket]): F[Unit] =
    copy(src, dst) >> remove(
      src,
      recursive = true
    ) // TODO: Copied recursive = true from gcs store. Does it make sense??

  override def copy(src: Url[Authority.Bucket], dst: Url[Authority.Bucket]): F[Unit] = {
    val (srcContainer, srcBlob) = AzureStore.urlToContainerAndBlob(src)
    val (dstContainer, dstBlob) = AzureStore.urlToContainerAndBlob(dst)
    val srcUrl = azure
      .getBlobContainerAsyncClient(srcContainer)
      .getBlobAsyncClient(srcBlob)
      .getBlobUrl
    val blobClient = azure
      .getBlobContainerAsyncClient(dstContainer)
      .getBlobAsyncClient(dstBlob)
    val copy = blobClient.beginCopy(srcUrl, Duration.of(1, ChronoUnit.SECONDS))
    liftJavaFuture(F.delay(copy.next().flatMap(_.getFinalResult).toFuture)).void
  }

  override def remove(url: Url[Authority.Bucket], recursive: Boolean = false): F[Unit] = {
    val (container, blobOrPrefix) = AzureStore.urlToContainerAndBlob(url)
    def recoverNotFound(m: Mono[Void]): Mono[Void] = m.onErrorResume(
      (t: Throwable) =>
        t match {
          case e: BlobStorageException => e.getStatusCode == 404
          case _                       => false
        },
      new JavaFunction[Throwable, Mono[Void]] {
        def apply(t: Throwable): Mono[Void] = Mono.empty()
      }
    )
    val containerClient = azure.getBlobContainerAsyncClient(container)
    val mono: Mono[Void] = if (recursive) {
      val options = {
        val opts = new ListBlobsOptions
        opts.setPrefix(blobOrPrefix)
      }
      containerClient.listBlobs(options).flatMap[Void] {
        new JavaFunction[BlobItem, Mono[Void]] {
          def apply(item: BlobItem): Mono[Void] =
            recoverNotFound(containerClient.getBlobAsyncClient(item.getName).delete())
        }
      }.ignoreElements()
    } else {
      recoverNotFound(containerClient.getBlobAsyncClient(blobOrPrefix).delete())
    }
    liftJavaFuture(F.delay(mono.toFuture)).void
  }

  override def putRotate(computePath: F[Url[Authority.Bucket]], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile = for {
      computed <- Resource.liftF(computePath)
      (container, blob) = AzureStore.urlToContainerAndBlob(computed)
      queue <- Resource.liftF(fs2.concurrent.Queue.bounded[F, Option[ByteBuffer]](queueSize))
      blobClient = azure
        .getBlobContainerAsyncClient(container)
        .getBlobAsyncClient(blob)
      pto    = new ParallelTransferOptions(blockSize, numBuffers, null) // scalafix:ok
      flux   = Flux.from(queue.dequeue.unNoneTerminate.toUnicastPublisher())
      upload = blobClient.upload(flux, pto, true)
      _ <- Resource.make(F.start(liftJavaFuture(F.delay(upload.toFuture)).void))(_.join)
      _ <- Resource.make(F.unit)(_ => queue.enqueue1(None))
    } yield queue
    putRotateBase(limit, openNewFile)(queue => bytes => queue.enqueue1(Some(bytes.toByteBuffer)))
  }

  override def stat(url: Url[Authority.Bucket]): F[Option[Path[AzureBlob]]] = {
    val (container, blobName) = AzureStore.urlToContainerAndBlob(url)
    val mono                  = azure.getBlobContainerAsyncClient(container).getBlobAsyncClient(blobName).getProperties
    liftJavaFuture(F.delay(mono.toFuture)).attempt.flatMap {
      case Right(props) =>
        val (bip, meta) = AzureStore.toBlobItemProperties(props)
        Path.of(blobName, AzureBlob(container, blobName, bip.some, meta)).some.pure[F]
      case Left(e: BlobStorageException) if e.getStatusCode == 404 =>
        none[Path[AzureBlob]].pure[F]
      case Left(e) => e.raiseError
    }
  }

  override def liftToUniversal: UniversalStore[F] =
    new Store.DelegatingStore[F, AzureBlob, Authority.Standard, UniversalFileSystemObject](
      FileSystemObject[AzureBlob].universal,
      Left(this)
    )

  def listUnderlying(
    url: Url[Authority.Bucket],
    fullMetadata: Boolean,
    expectTrailingSlashFiles: Boolean,
    recursive: Boolean
  ): Stream[F, Path[AzureBlob]] = {
    val (container, blobName) = AzureStore.urlToContainerAndBlob(url)
    val options = new ListBlobsOptions()
      .setPrefix(if (blobName == "/") "" else blobName)
      .setDetails(new BlobListDetails().setRetrieveMetadata(fullMetadata))
    val containerClient = azure.getBlobContainerAsyncClient(container)
    val blobPagedFlux =
      if (recursive) containerClient.listBlobs(options)
      else containerClient.listBlobsByHierarchy("/", options)
    val flux = blobPagedFlux
      .filter(bi => Option(bi.isDeleted).forall(deleted => !deleted))
      .flatMap[Path[AzureBlob]](AzureStore.blobItemToPath(containerClient, expectTrailingSlashFiles, container))
    fromPublisher(flux)
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
  )(implicit F: ConcurrentEffect[F], cs: ContextShift[F]): F[AzureStore[F]] =
    if (numBuffers < 2) new IllegalArgumentException(s"Number of buffers must be at least 2").raiseError
    else new AzureStore(azure, defaultFullMetadata, defaultTrailingSlashFiles, blockSize, numBuffers, queueSize).pure[F]

  private def urlToContainerAndBlob(url: Url[Authority.Bucket]): (String, String) =
    (url.authority.show, url.path.show.stripPrefix("/"))

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

  private def toHeaders(bip: BlobItemProperties): BlobHttpHeaders =
    new BlobHttpHeaders()
      .setCacheControl(bip.getCacheControl)
      .setContentDisposition(bip.getContentDisposition)
      .setContentEncoding(bip.getContentEncoding)
      .setContentLanguage(bip.getContentLanguage)
      .setContentMd5(bip.getContentMd5)
      .setContentType(bip.getContentType)

  private type MaybeBlobItemPropertiesMeta = Option[(BlobItemProperties, Map[String, String])]

  private def blobItemToPath(
    containerClient: BlobContainerAsyncClient,
    expectTrailingSlashFiles: Boolean,
    container: String
  ): JavaFunction[BlobItem, Mono[Path[AzureBlob]]] = new JavaFunction[BlobItem, Mono[Path[AzureBlob]]] {
    def recoverNotFound[T](m: Mono[T], fallback: T): Mono[T] = m.onErrorReturn(
      (t: Throwable) =>
        t match {
          case e: BlobStorageException => e.getStatusCode == 404
          case _                       => false
        },
      fallback
    )
    def apply(blobItem: BlobItem): Mono[Path[AzureBlob]] = {
      val properties = if (Option(blobItem.isPrefix: Boolean).contains(true) && expectTrailingSlashFiles) {
        val m = containerClient.getBlobAsyncClient(blobItem.getName).getProperties
          .map[AzureStore.MaybeBlobItemPropertiesMeta](new JavaFunction[BlobProperties, MaybeBlobItemPropertiesMeta] {
            def apply(bp: BlobProperties): Option[(BlobItemProperties, Map[String, String])] =
              AzureStore.toBlobItemProperties(bp).some
          })
        recoverNotFound(m, none)
      } else {
        val value = Option(blobItem.getProperties).map(
          _ -> Option(blobItem.getMetadata).fold(Map.empty[String, String])(_.asScala.toMap)
        )
        Mono.just(value)
      }
      properties.map[Path[AzureBlob]](new JavaFunction[MaybeBlobItemPropertiesMeta, Path[AzureBlob]] {
        def apply(propMeta: MaybeBlobItemPropertiesMeta): Path[AzureBlob] =
          Path(blobItem.getName).map { name =>
            AzureBlob(container, name, propMeta.map(_._1), propMeta.fold(Map.empty[String, String])(_._2))
          }
      })
    }

  }
}
