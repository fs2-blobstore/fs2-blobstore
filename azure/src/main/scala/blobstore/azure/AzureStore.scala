package blobstore
package azure

import blobstore.url.exception.Throwables
import blobstore.url.{Path, Url}
import cats.data.{Validated, ValidatedNec}
import cats.effect.{Async, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import com.azure.core.util.FluxUtil
import com.azure.storage.blob.batch.BlobBatchClientBuilder
import com.azure.storage.blob.{BlobContainerAsyncClient, BlobServiceAsyncClient}
import com.azure.storage.blob.models.*
import com.azure.storage.blob.specialized.BlockBlobAsyncClient
import com.azure.storage.common.implementation.Constants
import fs2.{Chunk, Pipe, Stream}
import fs2.interop.reactivestreams.*
import reactor.core.publisher.{Flux, Mono}

import java.nio.ByteBuffer
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.function.Function as JavaFunction
import scala.jdk.CollectionConverters.*

/** @param azure
  *   Azure Blob Service Async Client
  * @param defaultFullMetadata
  *   return full object metadata on [[list]], requires additional request per object. Metadata returned by default:
  *   size, lastModified, eTag, storageClass. This controls behaviour of [[list]] method from Store trait. Use
  *   [[listUnderlying]] to control on per-invocation basis.
  * @param defaultTrailingSlashFiles
  *   test if folders returned by [[list]] are files with trailing slashes in their names. This controls behaviour of
  *   [[list]] method from Store trait. Use [[listUnderlying]] to control on per-invocation basis.
  * @param blockSize
  *   for upload, The block size is the size of each block that will be staged. This value also determines the number of
  *   requests that need to be made. If block size is large, upload will make fewer network calls, but each individual
  *   call will send more data and will therefore take longer. This parameter also determines the size that each buffer
  *   uses when buffering is required and consequently amount of memory consumed by such methods may be up to blockSize
  *   * numBuffers.
  * @param numBuffers
  *   for buffered upload only, the number of buffers is the maximum number of buffers this method should allocate.
  *   Memory will be allocated lazily as needed. Must be at least two. Typically, the larger the number of buffers, the
  *   more parallel, and thus faster, the upload portion of this operation will be. The amount of memory consumed by
  *   methods using this value may be up to blockSize * numBuffers.
  */
class AzureStore[F[_]: Async](
  azure: BlobServiceAsyncClient,
  defaultFullMetadata: Boolean,
  defaultTrailingSlashFiles: Boolean,
  blockSize: Int,
  numBuffers: Int,
  queueSize: Int
) extends Store[F, AzureBlob] {

  override def list[A](url: Url[A], recursive: Boolean): Stream[F, Url[AzureBlob]] =
    listUnderlying(url, defaultFullMetadata, defaultTrailingSlashFiles, recursive)

  override def get[A](url: Url[A], chunkSize: Int): Stream[F, Byte] = {
    val (container, blobName) = AzureStore.urlToContainerAndBlob(url)
    fromPublisher(azure.getBlobContainerAsyncClient(container).getBlobAsyncClient(blobName).downloadStream(), 2)
      .flatMap(byteBuffer => Stream.chunk(Chunk.byteBuffer(byteBuffer)))
  }

  override def put[A](
    url: Url[A],
    overwrite: Boolean = true,
    size: Option[Long] = None
  ): Pipe[F, Byte, Unit] =
    put(
      url,
      overwrite,
      size.fold(none[BlobItemProperties])(s => new BlobItemProperties().setContentLength(s).some),
      Map.empty
    )

  def put[A](
    url: Url[A],
    overwrite: Boolean,
    properties: Option[BlobItemProperties],
    meta: Map[String, String]
  ): Pipe[F, Byte, Unit] = in =>
    Stream.resource(in.chunks.map(chunk => ByteBuffer.wrap(chunk.toArray)).toUnicastPublisher).flatMap { publisher =>
      val (container, blobName) = AzureStore.urlToContainerAndBlob(url)
      val blobClient            = azure
        .getBlobContainerAsyncClient(container)
        .getBlobAsyncClient(blobName)
      val flux = Flux.from(publisher)
      val pto  = new ParallelTransferOptions().setBlockSizeLong(blockSize.toLong).setMaxConcurrency(numBuffers.max(2))
      val (overwriteCheck, requestConditions) =
        if (overwrite) {
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
      Stream.eval(Async[F].fromCompletableFuture(Async[F].delay(upload.toFuture)).void)
    }

  override def move[A, B](src: Url[A], dst: Url[B]): F[Unit] =
    copy(src, dst) >> remove(src)

  override def copy[A, B](src: Url[A], dst: Url[B]): F[Unit] = {
    val (srcContainer, srcBlob) = AzureStore.urlToContainerAndBlob(src)
    val (dstContainer, dstBlob) = AzureStore.urlToContainerAndBlob(dst)
    val srcUrl                  = azure
      .getBlobContainerAsyncClient(srcContainer)
      .getBlobAsyncClient(srcBlob)
      .getBlobUrl
    val blobClient = azure
      .getBlobContainerAsyncClient(dstContainer)
      .getBlobAsyncClient(dstBlob)
    val copy = blobClient.beginCopy(srcUrl, Duration.of(1, ChronoUnit.SECONDS))
    Async[F].fromCompletableFuture(Async[F].delay(copy.next().flatMap(_.getFinalResult).toFuture)).void
  }

  override def remove[A](url: Url[A], recursive: Boolean = false): F[Unit] = {
    val (container, blobOrPrefix)                  = AzureStore.urlToContainerAndBlob(url)
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
    val containerClient  = azure.getBlobContainerAsyncClient(container)
    val mono: Mono[Void] =
      if (recursive) {
        val blobBatchClient = new BlobBatchClientBuilder(azure).buildAsyncClient()
        val options         = {
          val opts = new ListBlobsOptions
          opts.setPrefix(blobOrPrefix)
        }
        containerClient.listBlobs(options).buffer(256).flatMap {
          new JavaFunction[java.util.List[BlobItem], Flux[Void]] {
            def apply(blobs: java.util.List[BlobItem]): Flux[Void] = {
              val urls = blobs.asScala.map(b => containerClient.getBlobAsyncClient(b.getName).getBlobUrl).asJava
              blobBatchClient.deleteBlobs(urls, DeleteSnapshotsOptionType.INCLUDE).map(_.getValue)
            }
          }
        }.ignoreElements()
      } else {
        recoverNotFound(containerClient.getBlobAsyncClient(blobOrPrefix).delete())
      }
    Async[F].fromCompletableFuture(Async[F].delay(mono.toFuture)).void
  }

  override def putRotate[A](computeUrl: F[Url[A]], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile = for {
      computed <- Resource.eval(computeUrl)
      (container, blob) = AzureStore.urlToContainerAndBlob(computed)
      queue     <- Resource.eval(Queue.bounded[F, Option[ByteBuffer]](queueSize))
      publisher <- Stream.fromQueueNoneTerminated(queue).toUnicastPublisher
      blobClient = azure
        .getBlobContainerAsyncClient(container)
        .getBlobAsyncClient(blob)
      pto    = new ParallelTransferOptions().setBlockSizeLong(blockSize.toLong).setMaxConcurrency(numBuffers.max(2))
      flux   = Flux.from(publisher)
      upload = blobClient.upload(flux, pto, true)
      _ <-
        Resource.make(
          Async[F].start(Async[F].fromCompletableFuture(Async[F].delay(upload.toFuture)).void)
        )(
          _.join.flatMap(_.fold(Async[F].unit, e => Async[F].raiseError[Unit](e), identity))
        )
      _ <- Resource.make(Async[F].unit)(_ => queue.offer(None))
    } yield queue
    putRotateBase(limit, openNewFile)(queue => bytes => queue.offer(Some(ByteBuffer.wrap(bytes.toArray))))
  }

  override def stat[A](url: Url[A]): Stream[F, Url[AzureBlob]] = {
    val (container, blobName) = AzureStore.urlToContainerAndBlob(url)
    val mono                  = azure.getBlobContainerAsyncClient(container).getBlobAsyncClient(blobName).getProperties
    Stream.eval(Async[F].fromCompletableFuture(Async[F].delay(mono.toFuture)).attempt).evalMap {
      case Right(props) =>
        val (bip, meta) = AzureStore.toBlobItemProperties(props)
        val path        = Path.of(blobName, AzureBlob(container, blobName, bip.some, meta))
        url.withPath(path).some.pure[F]
      case Left(e: BlobStorageException) if e.getStatusCode == 404 =>
        none[Url[AzureBlob]].pure[F]
      case Left(e) => e.raiseError[F, Option[Url[AzureBlob]]]
    }.unNone
  }

  def listUnderlying[A](
    url: Url[A],
    fullMetadata: Boolean,
    expectTrailingSlashFiles: Boolean,
    recursive: Boolean
  ): Stream[F, Url[AzureBlob]] = {
    val (container, blobName) = AzureStore.urlToContainerAndBlob(url)
    val options               = new ListBlobsOptions()
      .setPrefix(if (blobName == "/") "" else blobName)
      .setDetails(new BlobListDetails().setRetrieveMetadata(fullMetadata))
    val containerClient = azure.getBlobContainerAsyncClient(container)
    val blobPagedFlux   =
      if (recursive) containerClient.listBlobs(options)
      else containerClient.listBlobsByHierarchy("/", options)
    val flux = blobPagedFlux
      .filter(bi => Option(bi.isDeleted).forall(deleted => !deleted))
      .flatMap[Path[AzureBlob]](AzureStore.blobItemToPath(containerClient, expectTrailingSlashFiles, container))
      .map[Url[AzureBlob]](new JavaFunction[Path[AzureBlob], Url[AzureBlob]] {
        def apply(p: Path[AzureBlob]): Url[AzureBlob] = url.copy(path = p)
      })
    fromPublisher(flux, 16)
  }
}

object AzureStore {

  def builder[F[_]: Async](client: BlobServiceAsyncClient): AzureStoreBuilder[F] = AzureStoreBuilderImpl[F](client)

  /** @see
    *   [[AzureStore]]
    */
  trait AzureStoreBuilder[F[_]] {
    def withClient(client: BlobServiceAsyncClient): AzureStoreBuilder[F]
    def withQueueSize(queueSize: Int): AzureStoreBuilder[F]
    def withBlockSize(blockSize: Int): AzureStoreBuilder[F]
    def withNumBuffers(numBuffers: Int): AzureStoreBuilder[F]
    def enableFullMetadata: AzureStoreBuilder[F]
    def disableFullMetadata: AzureStoreBuilder[F]
    def enableTrailingSlashFiles: AzureStoreBuilder[F]
    def disableTrailingSlashFiles: AzureStoreBuilder[F]
    def build: ValidatedNec[Throwable, AzureStore[F]]
    def unsafe: AzureStore[F] = build match {
      case Validated.Valid(a)    => a
      case Validated.Invalid(es) => throw es.reduce(Throwables.collapsingSemigroup) // scalafix:ok
    }
  }

  case class AzureStoreBuilderImpl[F[_]: Async](
    _azure: BlobServiceAsyncClient,
    _defaultFullMetadata: Boolean = false,
    _defaultTrailingSlashFiles: Boolean = false,
    _blockSize: Int = 50 * 1024 * 1024,
    _numBuffers: Int = 2,
    _queueSize: Int = 32
  ) extends AzureStoreBuilder[F] {
    def withClient(client: BlobServiceAsyncClient): AzureStoreBuilder[F] = this.copy(_azure = client)
    def withQueueSize(queueSize: Int): AzureStoreBuilder[F]              = this.copy(_queueSize = queueSize)
    def withBlockSize(blockSize: Int): AzureStoreBuilder[F]              = this.copy(_blockSize = blockSize)
    def withNumBuffers(numBuffers: Int): AzureStoreBuilder[F]            = this.copy(_numBuffers = numBuffers)
    def enableFullMetadata: AzureStoreBuilder[F]                         = this.copy(_defaultFullMetadata = true)
    def disableFullMetadata: AzureStoreBuilder[F]                        = this.copy(_defaultFullMetadata = false)
    def enableTrailingSlashFiles: AzureStoreBuilder[F]                   = this.copy(_defaultTrailingSlashFiles = true)
    def disableTrailingSlashFiles: AzureStoreBuilder[F]                  = this.copy(_defaultTrailingSlashFiles = false)

    def build: ValidatedNec[Throwable, AzureStore[F]] = {
      val validateBlockSize =
        if (_blockSize < 1 || _blockSize > BlockBlobAsyncClient.MAX_STAGE_BLOCK_BYTES_LONG) {
          new IllegalArgumentException(
            show"Block size must be in range [1, ${BlockBlobAsyncClient.MAX_STAGE_BLOCK_BYTES_LONG}]."
          ).invalidNec
        } else {
          ().validNec
        }

      val validateNumBuffers =
        if (_numBuffers < 2) {
          new IllegalArgumentException("Number of buffers should be at least 2").invalidNec
        } else ().validNec

      val validateQueueSize: ValidatedNec[IllegalArgumentException, Unit] =
        if (_queueSize < 4) {
          new IllegalArgumentException("Please use queue size of at least 4.").invalidNec
        } else ().validNec

      List(validateBlockSize, validateNumBuffers, validateQueueSize).combineAll.map(_ =>
        new AzureStore(_azure, _defaultFullMetadata, _defaultTrailingSlashFiles, _blockSize, _numBuffers, _queueSize)
      )
    }
  }

  private def urlToContainerAndBlob[A](url: Url[A]): (String, String) =
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
      val properties =
        if (Option(blobItem.isPrefix: Boolean).contains(true) && expectTrailingSlashFiles) {
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
        def apply(propMeta: MaybeBlobItemPropertiesMeta): Path[AzureBlob] = {
          val blob =
            AzureBlob(container, blobItem.getName, propMeta.map(_._1), propMeta.fold(Map.empty[String, String])(_._2))
          Path(blobItem.getName).as(blob)
        }
      })
    }

  }
}
