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

import blobstore.url.{Path, Url}
import cats.effect.std.{Hotswap, Queue}
import cats.effect.{Async, Resource}
import cats.syntax.all._
import fs2.{Chunk, INothing, Pipe, Pull, Stream}
import fs2.interop.reactivestreams._
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer, SdkPublisher}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/** @param s3 - S3 Async Client
  * @param objectAcl - optional default ACL to apply to all put, move and copy operations.
  * @param sseAlgorithm - optional default SSE Algorithm to apply to all put, move and copy operations.
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
class S3Store[F[_]: Async](
  s3: S3AsyncClient,
  objectAcl: Option[ObjectCannedACL] = None,
  sseAlgorithm: Option[String] = None,
  defaultFullMetadata: Boolean = false,
  defaultTrailingSlashFiles: Boolean = false,
  bufferSize: Int = S3Store.multiUploadDefaultPartSize.toInt,
  queueSize: Int = 32
) extends Store[F, S3Blob] {
  require(
    bufferSize >= S3Store.multiUploadMinimumPartSize,
    s"Buffer size must be at least ${S3Store.multiUploadMinimumPartSize}"
  )

  override def list[A](url: Url[A], recursive: Boolean = false): Stream[F, Url[S3Blob]] =
    listUnderlying(url, defaultFullMetadata, defaultTrailingSlashFiles, recursive)

  override def get[A](url: Url[A], chunkSize: Int): Stream[F, Byte] = {
    val bucket = url.authority.show
    val key    = url.path.relative.show

    performGet(GetObjectRequest.builder().bucket(bucket).key(key).build())
  }

  def get[A](url: Url[A], meta: S3MetaInfo): Stream[F, Byte] = {
    val bucket = url.authority.show
    val key    = url.path.relative.show

    val request = S3MetaInfo.getObjectRequest(meta).bucket(bucket).key(key).build()

    performGet(request)
  }

  private def performGet(request: GetObjectRequest) = {
    val cf = new CompletableFuture[Stream[F, Byte]]()
    val transformer: AsyncResponseTransformer[GetObjectResponse, Stream[F, Byte]] =
      new AsyncResponseTransformer[GetObjectResponse, Stream[F, Byte]] {
        override def prepare(): CompletableFuture[Stream[F, Byte]] = cf
        override def onResponse(response: GetObjectResponse): Unit = ()
        override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = {
          cf.complete(publisher.toStream.flatMap(bb => Stream.chunk(Chunk.byteBuffer(bb))))
          ()
        }
        override def exceptionOccurred(error: Throwable): Unit = {
          cf.completeExceptionally(error)
          ()
        }
      }
    Stream.eval(
      Async[F].fromCompletableFuture(Async[F].delay(s3.getObject[Stream[F, Byte]](request, transformer)))
    ).flatten
  }

  def put[A](url: Url[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] =
    put(url, overwrite, size, None)

  def put[A](
    url: Url[A],
    overwrite: Boolean,
    size: Option[Long],
    meta: Option[S3MetaInfo]
  ): Pipe[F, Byte, Unit] =
    in => {
      val bucket = url.authority.show
      val key    = url.path.relative.show

      val checkOverwrite =
        if (!overwrite) {
          Async[F].fromCompletableFuture(
            Async[F].delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()))
          ).attempt
            .flatMap {
              case Left(_: NoSuchKeyException) => Async[F].unit
              case Left(e)                     => Async[F].raiseError[Unit](e)
              case Right(_) =>
                Async[F].raiseError[Unit](new IllegalArgumentException(show"File at path '$url' already exist."))
            }
        } else Async[F].unit

      Stream.eval(checkOverwrite) ++
        size.fold(putUnknownSize(bucket, key, meta, in))(size => putKnownSize(bucket, key, meta, size, in))
    }

  override def move[A, B](src: Url[A], dst: Url[B]): F[Unit] =
    copy(src, dst) >> remove(src)

  override def copy[A, B](src: Url[A], dst: Url[B]): F[Unit] = {
    stat(dst).compile.last.flatMap(s => copy(src, dst, s.flatMap(_.representation.meta)))
  }

  def copy[A, B](src: Url[A], dst: Url[B], dstMeta: Option[S3MetaInfo]): F[Unit] = {
    val request = {
      val srcBucket = src.authority.show
      val srcKey    = src.path.relative.show
      val dstBucket = dst.authority.show
      val dstKey    = dst.path.relative.show

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
    Async[F].fromCompletableFuture(Async[F].delay(s3.copyObject(request))).void
  }

  override def remove[A](url: Url[A], recursive: Boolean = false): F[Unit] = {
    val bucket = url.authority.show
    val key    = url.path.relative.show
    val req    = DeleteObjectRequest.builder().bucket(bucket).key(key).build()

    if (recursive) new StoreOps[F, S3Blob](this).removeAll(url).void
    else Async[F].fromCompletableFuture(Async[F].delay(s3.deleteObject(req))).void
  }

  override def putRotate[A](computeUrl: F[Url[A]], limit: Long): Pipe[F, Byte, Unit] =
    if (limit <= bufferSize.toLong) {
      _.chunkN(limit.toInt)
        .flatMap(chunk => Stream.eval(computeUrl).flatMap(path => Stream.chunk(chunk).through(put(path))))
    } else {
      val newFile = for {
        path  <- Resource.eval(computeUrl)
        queue <- Resource.eval(Queue.bounded[F, Option[Chunk[Byte]]](queueSize))
        _ <- Resource.make(
          Async[F].start(
            Stream.fromQueueNoneTerminatedChunk(
              queue,
              // TODO: Remove this limit once https://github.com/aws/aws-sdk-java-v2/issues/953 is resolved.
              (limit / 2).toInt.min(S3Store.mb / 2)
            ).through(put(path)).compile.drain
          )
        )(_.join.flatMap(_.fold(Async[F].unit, e => Async[F].raiseError[Unit](e), identity)))
        _ <- Resource.make(Async[F].unit)(_ => queue.offer(None))
      } yield queue
      putRotateBase(limit, newFile)(queue => chunk => queue.offer(Some(chunk)))
    }

  def listUnderlying[A](
    url: Url[A],
    fullMetadata: Boolean,
    expectTrailingSlashFiles: Boolean,
    recursive: Boolean
  ): Stream[F, Url[S3Blob]] = {
    val bucket = url.authority.show
    val key    = url.path.relative.show
    val request = {
      val b = ListObjectsV2Request
        .builder()
        .bucket(bucket.show)
        .prefix(if (key == "/") "" else key)
      val builder = if (recursive) b else b.delimiter("/")
      builder.build()
    }
    s3.listObjectsV2Paginator(request).toStream.flatMap { ol =>
      val fDirs =
        ol.commonPrefixes().asScala.toList.flatMap(cp => Option(cp.prefix())).traverse[F, Path[S3Blob]] { prefix =>
          if (expectTrailingSlashFiles) {
            Async[F].fromCompletableFuture(
              Async[F].delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(prefix).build()))
            ).map { resp =>
              Path(prefix).as(S3Blob(bucket, prefix, new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some))
            }
              .recover {
                case _: NoSuchKeyException =>
                  Path(prefix).as(S3Blob(bucket, prefix, None))
              }
          } else {
            Path(prefix).as(S3Blob(bucket, prefix, None)).pure[F]
          }
        }

      val fFiles = ol.contents().asScala.toList.traverse[F, Path[S3Blob]] { s3Object =>
        if (fullMetadata) {
          Async[F].fromCompletableFuture(
            Async[F].delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(s3Object.key()).build()))
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
      (Stream.eval(fDirs).flatMap(dirs => Stream(dirs: _*)) ++
        Stream.eval(fFiles).flatMap(files => Stream(files: _*)))
        .map(p => url.copy(path = p))
    }
  }

  private def putKnownSize(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    size: Long,
    in: Stream[F, Byte]
  ): Stream[F, Unit] =
    Stream.resource(in.chunks.map(chunk => chunk.toByteBuffer).toUnicastPublisher).flatMap { publisher =>
      val request = S3Store.putObjectRequest(sseAlgorithm, objectAcl)(bucket, key, meta, size)

      val requestBody = AsyncRequestBody.fromPublisher(publisher)

      Stream.eval(Async[F].fromCompletableFuture(Async[F].delay(s3.putObject(request, requestBody))).void)
    }

  private def putSingleChunk(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    chunk: Chunk[Byte]
  ): Pull[F, INothing, Unit] =
    Pull.eval(Async[F].fromCompletableFuture(Async[F].delay(s3.putObject(
      S3Store.putObjectRequest(sseAlgorithm, objectAcl)(bucket, key, meta, chunk.size.toLong),
      AsyncRequestBody.fromByteBuffer(chunk.toByteBuffer)
    ))).void)

  private def putUnknownSize(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    in: Stream[F, Byte]
  ): Stream[F, Unit] =
    in.pull
      .unconsN(bufferSize, allowFewer = true)
      .flatMap {
        case None =>
          putSingleChunk(bucket, key, meta, Chunk.empty)
        case Some((chunk, _)) if chunk.size < bufferSize =>
          putSingleChunk(bucket, key, meta, chunk)
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
            Async[F].fromCompletableFuture(Async[F].delay(s3.createMultipartUpload(request)))

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
                  val fBody    = Async[F].delay(AsyncRequestBody.fromPublisher(CompletableFuturePublisher.from(cf)))
                  val done = for {
                    body <- fBody
                    resp <- Async[F].fromCompletableFuture(Async[F].delay(s3.uploadPart(req, body)))
                    _    <- Async[F].delay(byteBuffer.clear())
                  } yield CompletedPart.builder().eTag(resp.eTag()).partNumber(part).build()
                  Async[F].delay((cf, done))
                } {
                  case ((cf, _), Resource.ExitCase.Succeeded) =>
                    Async[F].delay { byteBuffer.flip(); cf.complete(byteBuffer) }.void
                  case ((cf, _), Resource.ExitCase.Errored(e)) =>
                    Async[F].delay(cf.completeExceptionally(e)).void
                  case ((cf, _), Resource.ExitCase.Canceled) =>
                    Async[F].delay(cf.cancel(true)).void
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
                      Pull.eval(
                        Async[F].fromCompletableFuture(Async[F].delay(s3.completeMultipartUpload(request)))
                      ).void
                    }
              }
              .onError {
                case _ =>
                  Pull.eval(Async[F].fromCompletableFuture(Async[F].delay(s3.abortMultipartUpload(abortReq)))).void
              }
          }
      }
      .stream

  override def stat[A](url: Url[A]): Stream[F, Url[S3Blob]] =
    Stream.eval(Async[F].fromCompletableFuture(Async[F].delay(
      s3.headObject(HeadObjectRequest.builder().bucket(url.authority.show).key(url.path.relative.show).build())
    )).map { resp =>
      val path = Path.of(
        url.path.show,
        S3Blob(url.authority.show, url.path.relative.show, new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some)
      )
      url.withPath(path).some
    }
      .recover {
        case _: NoSuchKeyException =>
          val meta = (!url.path.show.endsWith("/")).guard[Option].as(S3MetaInfo.const()).filterNot(_ =>
            defaultTrailingSlashFiles
          )
          val path = Path.of(url.path.show, S3Blob(url.authority.show, url.path.relative.show, meta))
          url.withPath(path).some
      }).unNone

}

object S3Store {

  /** @param s3 - S3 Async Client
    * @param objectAcl - optional default ACL to apply to all put, move and copy operations.
    * @param sseAlgorithm - optional default SSE Algorithm to apply to all put, move and copy operations.
    * @param defaultFullMetadata       – return full object metadata on [[S3Store.list]], requires additional request per object.
    *                                  Metadata returned by default: size, lastModified, eTag, storageClass.
    *                                  This controls behaviour of [[S3Store.list]] method from Store trait.
    *                                  Use [[S3Store.listUnderlying]] to control on per-invocation basis.
    * @param defaultTrailingSlashFiles - test if folders returned by [[S3Store.list]] are files with trailing slashes in their names.
    *                                  This controls behaviour of [[S3Store.list]] method from Store trait.
    *                                  Use [[S3Store.listUnderlying]] to control on per-invocation basis.
    * @param bufferSize - size of buffer for multipart uploading (used for large streams without size known in advance).
    *                     @see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
    */
  def apply[F[_]: Async](
    s3: S3AsyncClient,
    objectAcl: Option[ObjectCannedACL] = None,
    sseAlgorithm: Option[String] = None,
    defaultFullMetadata: Boolean = false,
    defaultTrailingSlashFiles: Boolean = false,
    bufferSize: Int = S3Store.multiUploadDefaultPartSize.toInt,
    queueSize: Int = 32
  ): F[S3Store[F]] =
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

  /** @see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
    */
  private val multiUploadMinimumPartSize: Long = 5L * mb
  private val multiUploadDefaultPartSize: Long = 500L * mb
  private val maxTotalSize: Long               = 5000000L * mb

  private case class MultipartState(partNumber: Int, totalBytes: Long)

  private def multipartGo[F[_]: Async](
    state: MultipartState,
    stream: Stream[F, Byte],
    byteBuffer: ByteBuffer,
    completion: F[CompletedPart],
    hotswap: Hotswap[F, F[CompletedPart]],
    newPart: (Int, Option[Long]) => Resource[F, F[CompletedPart]]
  ): Pull[F, CompletedPart, Unit] = {
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
            .eval(Async[F].delay(byteBuffer.put(hd.toByteBuffer)))
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
                .flatMap { newCompletion =>
                  hotswap.clear >> Async[F].delay(byteBuffer.limit(currentBytes)) >> newCompletion
                }
            )
            .flatMap(Pull.output1)
        } else {
          Pull.done
        }
    }
  }
}
