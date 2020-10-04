/*
Copyright 2018 LendUp Global, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package blobstore
package s3

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

import blobstore.url.{Authority, FileSystemObject, Path, Url}
import blobstore.Store.UniversalStore
import blobstore.url.general.UniversalFileSystemObject
import cats.effect.{ConcurrentEffect, ContextShift, ExitCase, Resource, Sync}
import cats.syntax.all._
import cats.instances.string._
import cats.instances.list._
import cats.instances.option._
import fs2.{Chunk, Hotswap, Pipe, Pull, Stream}
import fs2.interop.reactivestreams._
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer, SdkPublisher}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import util.liftJavaFuture

import scala.jdk.CollectionConverters._

/**
  * @param s3 - S3 Async Client
  * @param objectAcl - optional default ACL to apply to all [[put]], [[move]] and [[copy]] operations.
  * @param sseAlgorithm - optional default SSE Algorithm to apply to all [[put]], [[move]] and [[copy]] operations.
  * @param defaultFullMetadata       – return full object metadata on [[list]], requires additional request per object.
  *                                  Metadata returned by default: size, lastModified, eTag, storageClass.
  *                                  This controls behaviour of [[list]] method from Store trait.
  *                                  Use [[listUnderlying]] to control on per-invocation basis.
  * @param defaultTrailingSlashFiles - test if folders returned by [[list]] are files with trailing slashes in their names.
  *                                  This controls behaviour of [[list]] method from Store trait.
  *                                  Use [[listUnderlying]] to control on per-invocation basis.
  * @param bufferSize - size of buffer for multipart uploading (used for large streams without size known in advance).
  *                     @see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
  */
final class S3Store[F[_]](
  s3: S3AsyncClient,
  objectAcl: Option[ObjectCannedACL] = None,
  sseAlgorithm: Option[String] = None,
  defaultFullMetadata: Boolean = false,
  defaultTrailingSlashFiles: Boolean = false,
  bufferSize: Int = S3Store.multiUploadDefaultPartSize.toInt,
  queueSize: Int = 32
)(implicit F: ConcurrentEffect[F], cs: ContextShift[F])
  extends Store[F, Authority.Bucket, S3Blob] {
  require(
    bufferSize >= S3Store.multiUploadMinimumPartSize,
    s"Buffer size must be at least ${S3Store.multiUploadMinimumPartSize}"
  )

  override def list(path: Url[Authority.Bucket], recursive: Boolean = false): Stream[F, Path[S3Blob]] =
    listUnderlying(path, defaultFullMetadata, defaultTrailingSlashFiles, recursive)

  override def get(url: Url[Authority.Bucket], chunkSize: Int): Stream[F, Byte] = get(url, None)

  def get(url: Url[Authority.Bucket], meta: Option[S3MetaInfo]): Stream[F, Byte] = {
    val bucket = url.bucket.show
    val key    = url.path.show

    val request: GetObjectRequest =
      meta.fold(GetObjectRequest.builder())(S3MetaInfo.getObjectRequest).bucket(bucket).key(key).build()
    val cf = new CompletableFuture[Stream[F, Byte]]()
    val transformer: AsyncResponseTransformer[GetObjectResponse, Stream[F, Byte]] =
      new AsyncResponseTransformer[GetObjectResponse, Stream[F, Byte]] {
        override def prepare(): CompletableFuture[Stream[F, Byte]] = cf
        override def onResponse(response: GetObjectResponse): Unit = ()
        override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = {
          cf.complete(publisher.toStream().flatMap(bb => Stream.chunk(Chunk.byteBuffer(bb))))
          ()
        }
        override def exceptionOccurred(error: Throwable): Unit = {
          cf.completeExceptionally(error)
          ()
        }
      }
    Stream.eval(liftJavaFuture(F.delay(s3.getObject[Stream[F, Byte]](request, transformer)))).flatten
  }

  def put(url: Url[Authority.Bucket], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] =
    put(url, overwrite, size, None)

  def put(
    url: Url[Authority.Bucket],
    overwrite: Boolean,
    size: Option[Long],
    meta: Option[S3MetaInfo]
  ): Pipe[F, Byte, Unit] =
    in => {
      val bucket = url.bucket.show
      val key    = url.path.show

      val checkOverwrite = if (!overwrite) {
        liftJavaFuture(F.delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()))).attempt
          .flatMap {
            case Left(_: NoSuchKeyException) => F.unit
            case Left(e)                     => F.raiseError[Unit](e)
            case Right(_) =>
              F.raiseError[Unit](new IllegalArgumentException(s"File at path '$url' already exist."))
          }
      } else F.unit

      Stream.eval(checkOverwrite) ++
        size.fold(putUnknownSize(bucket, key, meta, in))(size => putKnownSize(bucket, key, meta, size, in))
    }

  override def move(src: Url[Authority.Bucket], dst: Url[Authority.Bucket]): F[Unit] =
    copy(src, dst) >> remove(src, recursive = false)

  override def copy(src: Url[Authority.Bucket], dst: Url[Authority.Bucket]): F[Unit] = {
    stat(dst).flatMap(s => copy(src, dst, s.flatMap(_.representation.meta)))
  }

  def copy(src: Url[Authority.Bucket], dst: Url[Authority.Bucket], dstMeta: Option[S3MetaInfo]): F[Unit] = {
    val request = {
      val srcBucket = src.bucket.show
      val srcKey    = src.path.show
      val dstBucket = dst.bucket.show
      val dstKey    = dst.path.show

      val builder = CopyObjectRequest.builder()
      val withAcl = objectAcl.fold(builder)(builder.acl)
      val withSSE = sseAlgorithm.fold(withAcl)(withAcl.serverSideEncryption)
      dstMeta
        .fold(withSSE)(S3MetaInfo.copyObjectRequest(withSSE))
        .copySource(srcBucket ++ "/" ++ srcKey)
        .destinationBucket(dstBucket)
        .destinationKey(dstKey)
        .build()
    }
    liftJavaFuture(F.delay(s3.copyObject(request))).void
  }

  override def remove(url: Url[Authority.Bucket], recursive: Boolean): F[Unit] = {
    val bucket = url.bucket.show
    val key    = url.path.show
    val req    = DeleteObjectRequest.builder().bucket(bucket).key(key).build()

    if (recursive) removeAll(url).void else liftJavaFuture(F.delay(s3.deleteObject(req))).void
  }

  override def putRotate(computePath: F[Url[Authority.Bucket]], limit: Long): Pipe[F, Byte, Unit] =
    if (limit <= bufferSize.toLong) {
      _.chunkN(limit.toInt)
        .flatMap(chunk => Stream.eval(computePath).flatMap(path => Stream.chunk(chunk).through(put(path))))
    } else {
      val newFile = for {
        path  <- Resource.liftF(computePath)
        queue <- Resource.liftF(fs2.concurrent.Queue.bounded[F, Option[Chunk[Byte]]](queueSize))
        _ <- Resource.make(
          F.start(queue.dequeue.unNoneTerminate.flatMap(Stream.chunk).through(put(path)).compile.drain)
        )(_.join)
        _ <- Resource.make(F.unit)(_ => queue.enqueue1(None))
      } yield queue
      putRotateBase(limit, newFile)(queue => chunk => queue.enqueue1(Some(chunk)))
    }

  def listUnderlying(
    path: Url[Authority.Bucket],
    fullMetadata: Boolean,
    expectTrailingSlashFiles: Boolean,
    recursive: Boolean
  ): Stream[F, Path[S3Blob]] = {
    val bucket = path.authority.show
    val key    = path.path.show
    val request = {
      val b = ListObjectsV2Request
        .builder()
        .bucket(bucket.show)
        .prefix(if (key == "/") "" else key)
      val builder = if (recursive) b else b.delimiter("/")
      builder.build()
    }
    s3.listObjectsV2Paginator(request).toStream().flatMap { ol =>
      val fDirs = ol.commonPrefixes().asScala.toList.traverse[F, Path[S3Blob]] { cp =>
        if (expectTrailingSlashFiles) {
          liftJavaFuture(
            F.delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(cp.prefix()).build()))
          ).map { resp =>
            Path(cp.prefix()).as(S3Blob(bucket, cp.prefix, new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some))
          }
            .recover {
              case _: NoSuchKeyException =>
                Path(cp.prefix()).as(S3Blob(bucket, cp.prefix(), None))
            }
        } else {
          Path(cp.prefix()).as(S3Blob(bucket, cp.prefix(), None)).pure[F]
        }
      }

      val fFiles = ol.contents().asScala.toList.traverse[F, Path[S3Blob]] { s3Object =>
        if (fullMetadata) {
          liftJavaFuture(
            F.delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(s3Object.key()).build()))
          ).map { resp =>
            Path(s3Object.key()).as(S3Blob(
              bucket,
              s3Object.key(),
              new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some
            ))
          }
        } else {
          Path(s3Object.key()).as(S3Blob(bucket, s3Object.key(), new S3MetaInfo.S3ObjectMetaInfo(s3Object).some)).pure[F]
        }
      }
      Stream.eval(fDirs).flatMap(dirs => Stream(dirs: _*)) ++ Stream
        .eval(fFiles)
        .flatMap(files => Stream(files: _*))
    }
  }

  private def putKnownSize(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    size: Long,
    in: Stream[F, Byte]
  ): Stream[F, Unit] = {
    val request = S3Store.putObjectRequest(sseAlgorithm, objectAcl)(bucket, key, meta, size)

    val requestBody = AsyncRequestBody.fromPublisher(in.chunks.map(chunk => chunk.toByteBuffer).toUnicastPublisher())

    Stream.eval(liftJavaFuture(F.delay(s3.putObject(request, requestBody))).void)
  }

  private def putUnknownSize(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    in: Stream[F, Byte]
  ): Stream[F, Unit] =
    in.pull
      .unconsN(bufferSize, allowFewer = true)
      .flatMap {
        case None => Pull.done
        case Some((chunk, _)) if chunk.size < bufferSize =>
          val request     = S3Store.putObjectRequest(sseAlgorithm, objectAcl)(bucket, key, meta, chunk.size.toLong)
          val requestBody = AsyncRequestBody.fromByteBuffer(chunk.toByteBuffer)
          Pull.eval(liftJavaFuture(F.delay(s3.putObject(request, requestBody))).void)
        case Some((chunk, rest)) =>
          val request: CreateMultipartUploadRequest = {
            val builder = CreateMultipartUploadRequest.builder()
            val withAcl = objectAcl.fold(builder)(builder.acl)
            val withSSE = sseAlgorithm.fold(withAcl)(withAcl.serverSideEncryption)
            meta
              .fold(withSSE)(S3MetaInfo.createMultipartUploadRequest(withSSE))
              .bucket(bucket)
              .key(key)
              .build()
          }

          val makeRequest: F[CreateMultipartUploadResponse] =
            liftJavaFuture(F.delay(s3.createMultipartUpload(request)))

          Pull.eval(makeRequest).flatMap { muResp =>
            val abortReq: AbortMultipartUploadRequest =
              AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(muResp.uploadId()).build()
            val uploadPartReqBuilder: UploadPartRequest.Builder = meta
              .fold(UploadPartRequest.builder())(S3MetaInfo.uploadPartRequest)
              .bucket(bucket)
              .key(key)
              .contentLength(bufferSize.toLong)
              .uploadId(muResp.uploadId())
            val completeReqBuilder = CompleteMultipartUploadRequest
              .builder()
              .bucket(bucket)
              .key(key)
              .uploadId(muResp.uploadId())

            val byteBuffer = ByteBuffer.allocateDirect(bufferSize)

            def mkNewPartResource(part: Int, size: Option[Long] = None): Resource[F, F[CompletedPart]] =
              Resource
                .makeCase {
                  val cf       = new CompletableFuture[ByteBuffer]()
                  val withPart = uploadPartReqBuilder.partNumber(part)
                  val req      = size.fold(withPart)(newSize => withPart.contentLength(newSize)).build()
                  val fBody    = F.delay(AsyncRequestBody.fromPublisher(CompletableFuturePublisher.from(cf)))
                  val done = for {
                    body <- fBody
                    resp <- liftJavaFuture(F.delay(s3.uploadPart(req, body)))
                    _    <- F.delay(byteBuffer.clear())
                  } yield CompletedPart.builder().eTag(resp.eTag()).partNumber(part).build()
                  F.delay((cf, done))
                } {
                  case ((cf, _), ExitCase.Completed) =>
                    F.delay { byteBuffer.flip(); cf.complete(byteBuffer) }.void
                  case ((cf, _), ExitCase.Error(e)) =>
                    F.delay(cf.completeExceptionally(e)).void
                  case ((cf, _), ExitCase.Canceled) =>
                    F.delay(cf.cancel(true)).void
                }
                .map(_._2)

            Stream
              .resource(Hotswap(mkNewPartResource(1)))
              .pull
              .headOrError
              .flatMap {
                case (hotswap, completion) =>
                  Pull
                    .eval(
                      S3Store
                        .multipartGo(
                          S3Store.MultipartState(1, 0L),
                          rest.cons(chunk),
                          byteBuffer,
                          completion,
                          hotswap,
                          mkNewPartResource
                        )
                        .stream
                        .compile
                        .toList
                    )
                    .flatMap { parts =>
                      val request =
                        completeReqBuilder
                          .multipartUpload(CompletedMultipartUpload.builder().parts(parts.asJava).build())
                          .build()
                      Pull.eval(liftJavaFuture(F.delay(s3.completeMultipartUpload(request)))).void
                    }
              }
              .onError {
                case _ =>
                  Pull.eval(liftJavaFuture(F.delay(s3.abortMultipartUpload(abortReq)))).void
              }
          }
      }
      .stream

  override def stat(url: Url[Authority.Bucket]): F[Option[Path[S3Blob]]] =
    liftJavaFuture(
      F.delay(s3.headObject(HeadObjectRequest.builder().bucket(url.bucket.show).key(url.path.show).build()))
    ).map { resp =>
      Path.of(
        url.path.show,
        S3Blob(url.bucket.show, url.path.show, new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some)
      ).some
    }
      .recover {
        case _: NoSuchKeyException =>
          val meta = (!url.path.show.endsWith("/")).guard[Option].as(S3MetaInfo.const()).filterNot(_ =>
            defaultTrailingSlashFiles
          )
          Path.of(url.path.show, S3Blob(url.bucket.show, url.path.show, meta)).some
      }

  override def liftToUniversal: UniversalStore[F] =
    new Store.DelegatingStore[F, S3Blob, Authority.Standard, UniversalFileSystemObject](
      FileSystemObject[S3Blob].universal,
      Left(this)
    )
}

object S3Store {
  def apply[F[_]](
    s3: S3AsyncClient,
    objectAcl: Option[ObjectCannedACL] = None,
    sseAlgorithm: Option[String] = None,
    defaultFullMetadata: Boolean = false,
    defaultTrailingSlashFiles: Boolean = false,
    bufferSize: Int = S3Store.multiUploadDefaultPartSize.toInt,
    queueSize: Int = 32
  )(implicit F: ConcurrentEffect[F], cs: ContextShift[F]): F[S3Store[F]] =
    if (bufferSize < S3Store.multiUploadMinimumPartSize) {
      new IllegalArgumentException(s"Buffer size must be at least ${S3Store.multiUploadMinimumPartSize}").raiseError
    } else
      new S3Store(
        s3,
        objectAcl,
        sseAlgorithm,
        defaultFullMetadata,
        defaultTrailingSlashFiles,
        bufferSize,
        queueSize
      ).pure[F]

  private def putObjectRequest(
    sseAlgorithm: Option[String],
    objectAcl: Option[ObjectCannedACL]
  )(bucket: String, key: String, meta: Option[S3MetaInfo], size: Long): PutObjectRequest = {
    val builder = PutObjectRequest.builder()
    val withAcl = objectAcl.fold(builder)(builder.acl)
    val withSSE = sseAlgorithm.fold(withAcl)(withAcl.serverSideEncryption)
    meta
      .fold(withSSE)(S3MetaInfo.putObjectRequest(withSSE))
      .contentLength(size)
      .bucket(bucket)
      .key(key)
      .build()
  }

  private val mb: Int = 1024 * 1024

  /**
    * @see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
    */
  private val multiUploadMinimumPartSize: Long = 5L * mb
  private val multiUploadDefaultPartSize: Long = 500L * mb
  private val maxTotalSize: Long               = 5000000L * mb

  private case class MultipartState(partNumber: Int, totalBytes: Long)

  private def multipartGo[F[_]](
    state: MultipartState,
    stream: Stream[F, Byte],
    byteBuffer: ByteBuffer,
    completion: F[CompletedPart],
    hotswap: Hotswap[F, F[CompletedPart]],
    newPart: (Int, Option[Long]) => Resource[F, F[CompletedPart]]
  )(implicit F: Sync[F]): Pull[F, CompletedPart, Unit] = {
    stream.pull.unconsLimit(byteBuffer.capacity() - byteBuffer.position()).flatMap {
      case Some((hd, tl)) =>
        val newTotal = state.totalBytes + hd.size
        if (newTotal > maxTotalSize) {
          Pull.raiseError(
            new IllegalArgumentException(
              s"S3 doesn't support files larger than 5 TB, stream has at least $newTotal bytes"
            )
          )
        } else {
          Pull
            .eval(F.delay(byteBuffer.put(hd.toByteBuffer)))
            .flatMap { _ =>
              if (byteBuffer.position() < byteBuffer.capacity()) {
                multipartGo(
                  state.copy(totalBytes = newTotal),
                  stream = tl,
                  byteBuffer = byteBuffer,
                  completion = completion,
                  hotswap = hotswap,
                  newPart = newPart
                )
              } else {
                Pull
                  .eval(hotswap.swap(newPart(state.partNumber + 1, None)))
                  .flatMap { newCompletion =>
                    Pull.eval(completion).flatMap { cp =>
                      Pull.output1[F, CompletedPart](cp) >>
                        multipartGo(
                          MultipartState(state.partNumber + 1, newTotal),
                          stream = tl,
                          byteBuffer = byteBuffer,
                          completion = newCompletion,
                          hotswap = hotswap,
                          newPart = newPart
                        )
                    }
                  }
              }
            }
        }
      case None =>
        val currentBytes = byteBuffer.position()
        if (currentBytes > 0) {
          Pull
            .eval(
              hotswap
                .swap(newPart(state.partNumber, Some(currentBytes.toLong)))
                .flatMap { newCompletion => hotswap.clear >> F.delay(byteBuffer.limit(currentBytes)) >> newCompletion }
            )
            .flatMap(Pull.output1)
        } else {
          Pull.done
        }
    }
  }
}
