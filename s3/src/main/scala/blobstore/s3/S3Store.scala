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

import blobstore.url.exception.Throwables
import blobstore.url.{Path, Url}
import blobstore.util.{fromQueueNoneTerminated, fromQueueNoneTerminatedChunk, liftJavaFuture}
import cats.data.{Validated, ValidatedNec}
import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Async, ConcurrentEffect, ExitCase, Resource, Timer}
import cats.syntax.all.*
import fs2.concurrent.Queue
import fs2.{Chunk, Pipe, Pull, Stream}
import fs2.interop.reactivestreams.*
import org.reactivestreams.Publisher
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer, SdkPublisher}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.*

import java.nio.{Buffer, ByteBuffer}
import java.util.concurrent.CompletableFuture
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

/** @param s3
  *   S3 Async Client.
  * @param crtClient
  *   optional instance S3CrtAsyncClient, which is used by S3 Transfer Manager (Requires additional runtime dependency,
  *   but may lead to faster uploads/downloads).
  * @param objectAcl
  *   optional default ACL to apply to all put, move and copy operations.
  * @param sseAlgorithm
  *   optional default SSE Algorithm to apply to all put, move and copy operations.
  * @param defaultFullMetadata
  *   – return full object metadata on [[list]], requires additional request per object. Metadata returned by default:
  *   size, lastModified, eTag, storageClass. This controls behaviour of [[list]] method from Store trait. Use
  *   [[listUnderlying]] to control on per-invocation basis.
  * @param defaultTrailingSlashFiles
  *   - test if folders returned by [[list]] are files with trailing slashes in their names. This controls behaviour of
  *     [[list]] method from Store trait. Use [[listUnderlying]] to control on per-invocation basis.
  * @param bufferSize
  *   – size of the buffer for multipart uploading (used for large streams without size known in advance).
  * @see
  *   https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
  */
class S3Store[F[_]: ConcurrentEffect: Timer](
  s3: S3AsyncClient,
  crtClient: Option[S3AsyncClient],
  objectAcl: Option[ObjectCannedACL],
  sseAlgorithm: Option[ServerSideEncryption],
  defaultFullMetadata: Boolean,
  defaultTrailingSlashFiles: Boolean,
  bufferSize: Int,
  queueSize: Int
) extends Store[F, S3Blob] {

  def bestClient: S3AsyncClient = crtClient.getOrElse(s3)

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

    performGet(S3MetaInfo.mkGetObjectRequest(bucket, key, meta))
  }

  private def performGet(request: GetObjectRequest) = {
    val cf = new CompletableFuture[Publisher[ByteBuffer]]()
    val transformer: AsyncResponseTransformer[GetObjectResponse, Publisher[ByteBuffer]] =
      new AsyncResponseTransformer[GetObjectResponse, Publisher[ByteBuffer]] {
        override def prepare(): CompletableFuture[Publisher[ByteBuffer]] = cf
        override def onResponse(response: GetObjectResponse): Unit       = ()
        override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = {
          cf.complete(publisher)
          ()
        }
        override def exceptionOccurred(error: Throwable): Unit = {
          cf.completeExceptionally(error)
          ()
        }
      }
    Stream.eval(
      liftJavaFuture(ConcurrentEffect[F].delay(bestClient.getObject(request, transformer)))
    ).flatMap(publisher => publisher.toStream.flatMap(bb => Stream.chunk(Chunk.byteBuffer(bb))))
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
          liftJavaFuture(
            ConcurrentEffect[F].delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()))
          ).attempt
            .flatMap {
              case Left(_: NoSuchKeyException) => Async[F].unit
              case Left(e)                     => Async[F].raiseError[Unit](e)
              case Right(_) =>
                Async[F].raiseError[Unit](new IllegalArgumentException(show"File at path '$url' already exist."))
            }
        } else Async[F].unit

      Stream.eval(checkOverwrite) ++ putUnderlying(bucket, key, meta, size, in)
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

      S3MetaInfo.mkCopyObjectRequest(sseAlgorithm, objectAcl, srcBucket, srcKey, dstBucket, dstKey, dstMeta)
    }
    liftJavaFuture(Async[F].delay(s3.copyObject(request))).void
  }

  override def remove[A](url: Url[A], recursive: Boolean = false): F[Unit] = {
    val bucket = url.authority.show

    if (recursive) {
      list(url, recursive).groupWithin(1000, FiniteDuration(1, "ms")).evalMap { chunk =>
        val objects = chunk.map(u => ObjectIdentifier.builder().key(u.path.relative.show).build()).toList
        val req =
          DeleteObjectsRequest.builder().bucket(bucket).delete(Delete.builder().objects(objects.asJava).build()).build()
        liftJavaFuture(Async[F].delay(s3.deleteObjects(req))).flatMap[Unit] {
          case resp if resp.hasErrors =>
            def msg(e: S3Error): String = show"S3 error(${e.code()}) – ${e.message()}"
            resp.errors().asScala.toList match {
              case Nil      => Async[F].unit
              case e :: Nil => Async[F].raiseError(new RuntimeException(msg(e)))
              case es       => Async[F].raiseError(new RuntimeException(es.map(msg).mkString("Errors: [", ", ", "]")))
            }
          case _ => Async[F].unit
        }
      }.compile.drain
    } else {
      val key = url.path.relative.show
      val req = DeleteObjectRequest.builder().bucket(bucket).key(key).build()
      liftJavaFuture(Async[F].delay(s3.deleteObject(req))).void
    }
  }

  override def putRotate[A](computeUrl: F[Url[A]], limit: Long): Pipe[F, Byte, Unit] = {
    val newFile = for {
      path  <- Resource.eval(computeUrl)
      queue <- Resource.eval(Queue.bounded[F, Option[Chunk[Byte]]](queueSize))
      _ <- Resource.make(
        ConcurrentEffect[F].start(
          fromQueueNoneTerminatedChunk(queue).through(put(path)).compile.drain
        )
      )(_.join)
      _ <- Resource.make(Async[F].unit)(_ => queue.enqueue1(None))
    } yield queue
    putRotateBase(limit, newFile)(queue => chunk => queue.enqueue1(Some(chunk)))
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
            liftJavaFuture(
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
          liftJavaFuture(
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
      (Stream.eval(fDirs).flatMap(dirs => Stream(dirs*)) ++
        Stream.eval(fFiles).flatMap(files => Stream(files*)))
        .map(p => url.copy(path = p))
    }
  }

  private def putSingle(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    bytes: Array[Byte]
  ): F[Unit] = {
    val request     = S3MetaInfo.mkPutObjectRequest(sseAlgorithm, objectAcl, bucket, key, meta, bytes.length.toLong)
    val requestBody = AsyncRequestBody.fromBytes(bytes)
    liftJavaFuture(Async[F].delay(bestClient.putObject(request, requestBody))).void
  }

  private def _putMultiPart(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    maybeSize: Option[Long],
    in: Stream[F, Byte]
  ): Stream[F, Unit] = {
    val request: CreateMultipartUploadRequest =
      S3MetaInfo.mkPutMultiPartRequest(sseAlgorithm, objectAcl, bucket, key, meta)

    val makeRequest: F[CreateMultipartUploadResponse] =
      liftJavaFuture(Async[F].delay(s3.createMultipartUpload(request)))

    Stream.eval((makeRequest, Semaphore(2)).tupled).flatMap { case (muResp, semaphore) =>
      val partRef                                        = Ref.unsafe(1)
      val completedPartsRef: Ref[F, List[CompletedPart]] = Ref.unsafe(Nil)

      val pipe: Pipe[F, Byte, Unit] = maybeSize match {
        case Some(size) =>
          val partSize     = size.max(S3Store.multiUploadMinimumPartSize).min(S3Store.multiUploadDefaultPartSize)
          val totalParts   = (size.toDouble / partSize).ceil.toInt
          val lastPartSize = size - ((totalParts - 1) * partSize)
          val resource = for {
            part <- Resource.eval(partRef.getAndUpdate(_ + 1))
            _ <- Resource.eval(if (part > totalParts)
              new IllegalArgumentException("Provided size doesn't match evaluated stream length.").raiseError
            else Async[F].unit)
            queue <- Resource.eval(Queue.bounded[F, Option[ByteBuffer]](queueSize))
            publisher = fromQueueNoneTerminated(queue).toUnicastPublisher
            _ <- Resource.make(
              ConcurrentEffect[F].start(
                liftJavaFuture(Async[F].delay {
                  s3.uploadPart(
                    S3MetaInfo.mkUploadPartRequestBuilder(
                      bucket,
                      key,
                      muResp.uploadId(),
                      meta,
                      part,
                      Some(if (part == totalParts) lastPartSize else partSize)
                    ),
                    AsyncRequestBody.fromPublisher(publisher)
                  )
                })
              )
            )(_.join.flatMap(resp =>
              completedPartsRef.update(CompletedPart.builder().eTag(resp.eTag()).partNumber(part).build() :: _)
            ))
            _ <- Resource.make(Async[F].unit)(_ => queue.enqueue1(None))
          } yield queue

          if (totalParts > S3Store.maxMultipartParts) {
            _ => Stream.raiseError(S3Store.multipartUploadPartsError)
          } else {
            putRotateBase(partSize, resource) { queue => chunk =>
              queue.enqueue1(Some(ByteBuffer.wrap(chunk.toArray)))
            }
          }
        case None =>
          lazy val directBuffers = (
            ByteBuffer.allocateDirect(bufferSize),
            ByteBuffer.allocateDirect(bufferSize)
          )

          def selectBuffer(part: Int) = if (part % 2 == 0) directBuffers._1 else directBuffers._2

          def acquireBuffer(part: Int) = for {
            _ <- semaphore.acquire
            _ <- ConcurrentEffect[F].delay {
              selectBuffer(part).asInstanceOf[Buffer].clear() // scalafix:ok
            }
          } yield ()

          val resource = for {
            part <- Resource.eval(partRef.getAndUpdate(_ + 1))
            _ <- Resource.eval {
              if (part > S3Store.maxMultipartParts) S3Store.multipartUploadPartsError.raiseError
              else Async[F].unit
            }
            _ <- Resource.make(acquireBuffer(part))(_ => semaphore.release)
            _ <- Resource.make(ConcurrentEffect[F].unit) { _ =>
              liftJavaFuture(Async[F].delay {
                s3.uploadPart(
                  S3MetaInfo.mkUploadPartRequestBuilder(
                    bucket,
                    key,
                    muResp.uploadId(),
                    meta,
                    part,
                    None
                  ),
                  AsyncRequestBody.fromByteBuffer(
                    selectBuffer(part).asInstanceOf[Buffer].flip().asInstanceOf[ByteBuffer] // scalafix:ok
                  )
                )
              }).flatMap { resp =>
                completedPartsRef.update(CompletedPart.builder().eTag(resp.eTag()).partNumber(part).build() :: _)
              }
            }
          } yield selectBuffer(part)

          putRotateBase(bufferSize.toLong, resource)(bb =>
            chunk => ConcurrentEffect[F].delay(bb.put(chunk.toArray)).void
          )
      }

      in.through(pipe).onFinalizeCase {
        case ExitCase.Completed =>
          completedPartsRef.get.flatMap { parts =>
            val req = CompleteMultipartUploadRequest
              .builder()
              .bucket(bucket)
              .key(key)
              .uploadId(muResp.uploadId())
              .multipartUpload(CompletedMultipartUpload.builder().parts(parts.reverse.asJava).build())
              .build()

            liftJavaFuture(Async[F].delay(s3.completeMultipartUpload(req))).void
          }
        case _ =>
          val req = AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(muResp.uploadId()).build()
          liftJavaFuture(Async[F].delay(s3.abortMultipartUpload(req))).void
      }
    }
  }

  private def putMultiPart(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    maybeSize: Option[Long],
    in: Stream[F, Byte]
  ): Stream[F, Unit] = maybeSize match {
    case Some(knownSize) if crtClient.isDefined =>
      val publisher = in.chunks.map(chunk => ByteBuffer.wrap(chunk.toArray)).toUnicastPublisher
      val body      = AsyncRequestBody.fromPublisher(publisher)
      val request   = S3MetaInfo.mkPutObjectRequest(sseAlgorithm, objectAcl, bucket, key, meta, knownSize)
      Stream.eval(liftJavaFuture(Async[F].delay(bestClient.putObject(request, body))).void)
    case maybeSize => _putMultiPart(bucket, key, meta, maybeSize, in)
  }

  private def putUnderlying(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    maybeSize: Option[Long],
    in: Stream[F, Byte]
  ): Stream[F, Unit] = {
    maybeSize match {
      case None =>
        in.pull.unconsN(S3Store.multiUploadMinimumPartSize.toInt, allowFewer = true).flatMap {
          case None =>
            Pull.eval(putSingle(bucket, key, meta, Array.emptyByteArray))
          case Some((chunk, _)) if chunk.size < S3Store.multiUploadMinimumPartSize.toInt =>
            Pull.eval(putSingle(bucket, key, meta, chunk.toArray))
          case Some((chunk, rest)) =>
            Pull.eval(putMultiPart(bucket, key, meta, none, rest.consChunk(chunk)).compile.drain)
        }.stream
      case Some(size) if size <= S3Store.multiUploadThreshold =>
        Stream.eval(in.compile.to(Array).flatMap(bytes => putSingle(bucket, key, meta, bytes = bytes)))
      case size =>
        putMultiPart(bucket, key, meta, size, in)
    }
  }

  override def stat[A](url: Url[A]): Stream[F, Url[S3Blob]] =
    Stream.eval(liftJavaFuture(Async[F].delay(
      s3.headObject(HeadObjectRequest.builder().bucket(url.authority.show).key(url.path.relative.show).build())
    )).map { resp =>
      val path = Path.of(
        url.path.show,
        S3Blob(url.authority.show, url.path.relative.show, new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some)
      )
      url.withPath(path).some
    }.recover { case _: NoSuchKeyException => None }).unNone

}

object S3Store {

  def builder[F[_]: ConcurrentEffect: Timer](s3: S3AsyncClient): S3StoreBuilder[F] = S3StoreBuilderImpl[F](s3)

  private val mb: Int = 1024 * 1024

  /** @see
    *   https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
    */
  private val multiUploadMinimumPartSize: Long = 5L * mb
  private val multiUploadThreshold: Long       = 100L * mb
  private val multiUploadDefaultPartSize: Long = 500L * mb
  private val maxMultipartParts: Int           = 10000

  private val multipartUploadPartsError = new IllegalArgumentException(
    show"S3 doesn't support multipart uploads with more than ${S3Store.maxMultipartParts} parts."
  )

  /** @see
    *   [[S3Store]]
    */
  trait S3StoreBuilder[F[_]] {
    def withS3Client(s3Client: S3AsyncClient): S3StoreBuilder[F]
    def setCrtClient(maybeCrtClient: Option[S3AsyncClient]): S3StoreBuilder[F]
    def setObjectAcl(maybeObjectAcl: Option[ObjectCannedACL]): S3StoreBuilder[F]
    def setSseAlgorithm(maybeSseAlgorithm: Option[ServerSideEncryption]): S3StoreBuilder[F]
    def withBufferSize(bufferSize: Int): S3StoreBuilder[F]
    def withQueueSize(queueSize: Int): S3StoreBuilder[F]
    def enableFullMetadata: S3StoreBuilder[F]
    def disableFullMetadata: S3StoreBuilder[F]
    def enableTrailingSlashFiles: S3StoreBuilder[F]
    def disableTrailingSlashFiles: S3StoreBuilder[F]
    def withCrtClient(crtClient: S3AsyncClient): S3StoreBuilder[F]              = setCrtClient(Some(crtClient))
    def withObjectAcl(objectAcl: ObjectCannedACL): S3StoreBuilder[F]            = setObjectAcl(Some(objectAcl))
    def withSseAlgorithm(sseAlgorithm: ServerSideEncryption): S3StoreBuilder[F] = setSseAlgorithm(Some(sseAlgorithm))
    def build: ValidatedNec[Throwable, S3Store[F]]
    def unsafe: S3Store[F] = build match {
      case Validated.Valid(a)    => a
      case Validated.Invalid(es) => throw es.reduce(Throwables.collapsingSemigroup) // scalafix:ok
    }
  }

  case class S3StoreBuilderImpl[F[_]: ConcurrentEffect: Timer](
    _s3Client: S3AsyncClient,
    _crtClient: Option[S3AsyncClient] = None,
    _objectAcl: Option[ObjectCannedACL] = None,
    _sseAlgorithm: Option[ServerSideEncryption] = None,
    _defaultFullMetadata: Boolean = false,
    _defaultTrailingSlashFiles: Boolean = false,
    _bufferSize: Int = S3Store.multiUploadDefaultPartSize.toInt,
    _queueSize: Int = 32
  ) extends S3StoreBuilder[F] {
    def withS3Client(s3Client: S3AsyncClient): S3StoreBuilder[F]               = this.copy(_s3Client = s3Client)
    def setCrtClient(maybeCrtClient: Option[S3AsyncClient]): S3StoreBuilder[F] = this.copy(_crtClient = maybeCrtClient)
    def setObjectAcl(maybeObjectAcl: Option[ObjectCannedACL]): S3StoreBuilder[F] =
      this.copy(_objectAcl = maybeObjectAcl)
    def setSseAlgorithm(maybeSseAlgorithm: Option[ServerSideEncryption]): S3StoreBuilder[F] =
      this.copy(_sseAlgorithm = maybeSseAlgorithm)
    def withBufferSize(bufferSize: Int): S3StoreBuilder[F] = this.copy(_bufferSize = bufferSize)
    def withQueueSize(queueSize: Int): S3StoreBuilder[F]   = this.copy(_queueSize = queueSize)
    def enableFullMetadata: S3StoreBuilder[F]              = this.copy(_defaultFullMetadata = true)
    def disableFullMetadata: S3StoreBuilder[F]             = this.copy(_defaultFullMetadata = false)
    def enableTrailingSlashFiles: S3StoreBuilder[F]        = this.copy(_defaultTrailingSlashFiles = true)
    def disableTrailingSlashFiles: S3StoreBuilder[F]       = this.copy(_defaultTrailingSlashFiles = false)

    def build: ValidatedNec[Throwable, S3Store[F]] = {
      val validateCrtClient: ValidatedNec[IllegalArgumentException, Unit] = _crtClient match {
        case Some(client) if !checkExpectedCrtClientClass(client) =>
          new IllegalArgumentException(
            "CRT client must implement software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient."
          ).invalidNec
        case _ => ().validNec
      }

      val validateBufferSize: ValidatedNec[IllegalArgumentException, Unit] =
        if (_bufferSize < multiUploadMinimumPartSize) {
          new IllegalArgumentException(
            "Please use buffer size of at least 5Mb – S3 requires minimal size of 5Mb per part of multipart upload."
          ).invalidNec
        } else if (_bufferSize > multiUploadDefaultPartSize) {
          new IllegalArgumentException(
            "Please use buffer size less than 500Mb – S3 requires maximum object size of 5Tb, and maximum number of 10000 parts in multipart upload."
          ).invalidNec
        } else ().validNec

      val validateQueueSize: ValidatedNec[IllegalArgumentException, Unit] =
        if (_queueSize < 4) {
          new IllegalArgumentException("Please use queue size of at least 4.").invalidNec
        } else ().validNec

      List(validateCrtClient, validateBufferSize, validateQueueSize).combineAll.map(_ =>
        new S3Store[F](
          _s3Client,
          _crtClient,
          _objectAcl,
          _sseAlgorithm,
          _defaultFullMetadata,
          _defaultTrailingSlashFiles,
          _bufferSize,
          _queueSize
        )
      )
    }

    private def checkExpectedCrtClientClass(client: S3AsyncClient): Boolean =
      client.getClass.getInterfaces.map(_.getCanonicalName).contains(
        "software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient"
      )
  }

}
