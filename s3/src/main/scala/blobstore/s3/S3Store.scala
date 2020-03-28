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
import java.util.concurrent.{CancellationException, CompletableFuture, CompletionException}

import cats.effect.{Concurrent, ConcurrentEffect, ExitCase, Resource, Sync}
import cats.syntax.all._
import cats.instances.string._
import cats.instances.list._
import fs2.{Chunk, Hotswap, Pipe, Pull, Stream}
import fs2.interop.reactivestreams._
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer, SdkPublisher}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._

import scala.jdk.CollectionConverters._

/**
  * @param s3 - S3 Async Client
  * @param objectAcl - optional default ACL to apply to all [[put]], [[move]] and [[copy]] operations.
  * @param sseAlgorithm - optional default SSE Algorithm to apply to all [[put]], [[move]] and [[copy]] operations.
  * @param defaultFullMetadata       – return full object metadata on [[list]], requires additional request per object.
  *                                  Metadata returned by default: size, lastModified, eTag, storageClass.
  *                                  This controls behaviour of [[list]] method from [[Store]] trait.
  *                                  Use [[listUnderlying]] to control on per-invocation basis.
  * @param defaultTrailingSlashFiles - test if folders returned by [[list]] are files with trailing slashes in their names.
  *                                  This controls behaviour of [[list]] method from [[Store]] trait.
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
  bufferSize: Int = S3Store.multiUploadDefaultPartSize.toInt
)(implicit F: ConcurrentEffect[F])
  extends Store[F] {
  require(
    bufferSize >= S3Store.multiUploadMinimumPartSize,
    s"Buffer size must be at least ${S3Store.multiUploadMinimumPartSize}"
  )

  override def list(path: Path): Stream[F, Path] = listUnderlying(path, defaultFullMetadata, defaultTrailingSlashFiles)

  override def get(path: Path, chunkSize: Int): Stream[F, Byte] = S3Store.bucketKeyMetaFromPath(path) match {
    case None => Stream.raiseError(S3Store.missingRootError(s"Unable to read '$path'"))
    case Some((bucket, key, meta)) =>
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
      Stream.eval(S3Store.liftJavaFuture(F.delay(s3.getObject[Stream[F, Byte]](request, transformer)))).flatten
  }

  override def put(path: Path): Pipe[F, Byte, Unit] =
    in =>
      S3Store.bucketKeyMetaFromPath(path) match {
        case None => Stream.raiseError(S3Store.missingRootError(s"Unable to write to '$path'"))
        case Some((bucket, key, meta)) =>
          path.size.fold(putUnknownSize(bucket, key, meta, in))(size => putKnownSize(bucket, key, meta, size, in))
      }

  override def move(src: Path, dst: Path): F[Unit] =
    copy(src, dst) *> remove(src)

  override def copy(src: Path, dst: Path): F[Unit] =
    for {
      (srcBucket, srcKey, _) <- S3Store
        .bucketKeyMetaFromPath(src)
        .fold[F[(String, String, Option[S3MetaInfo])]](S3Store.missingRootError(s"Wrong src '$src'").raiseError)(
          _.pure[F]
        )
      (dstBucket, dstKey, dstMeta) <- S3Store
        .bucketKeyMetaFromPath(dst)
        .fold[F[(String, String, Option[S3MetaInfo])]](S3Store.missingRootError(s"Wrong dst '$dst'").raiseError)(
          _.pure[F]
        )
      _ <- {
        val request = {
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
        S3Store.liftJavaFuture(F.delay(s3.copyObject(request))).void
      }
    } yield ()

  override def remove(path: Path): F[Unit] = S3Store.bucketKeyMetaFromPath(path) match {
    case None => S3Store.missingRootError(s"Unable to remove '$path'").raiseError[F, Unit]
    case Some((bucket, key, _)) =>
      val req = DeleteObjectRequest.builder().bucket(bucket).key(key).build()
      S3Store.liftJavaFuture(F.delay(s3.deleteObject(req))).void
  }

  def listUnderlying(path: Path, fullMetadata: Boolean, expectTrailingSlashFiles: Boolean): Stream[F, Path] =
    S3Store.bucketKeyMetaFromPath(path) match {
      case None => Stream.raiseError(S3Store.missingRootError(s"Unable to list '$path'"))
      case Some((bucket, key, _)) =>
        val request = ListObjectsV2Request
          .builder()
          .bucket(bucket)
          .prefix(if (key == "/") "" else key)
          .delimiter("/")
          .build()
        s3.listObjectsV2Paginator(request).toStream().flatMap { ol =>
          val fDirs = ol.commonPrefixes().asScala.toList.traverse[F, S3Path] { cp =>
            if (expectTrailingSlashFiles) {
              S3Store
                .liftJavaFuture(
                  F.delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(cp.prefix()).build()))
                )
                .map { resp => S3Path(bucket, cp.prefix, new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some) }
                .recover {
                  case _: NoSuchKeyException =>
                    S3Path(bucket, cp.prefix(), None)
                }
            } else {
              S3Path(bucket, cp.prefix(), None).pure[F]
            }
          }

          val fFiles = ol.contents().asScala.toList.traverse[F, S3Path] { s3Object =>
            if (fullMetadata) {
              S3Store
                .liftJavaFuture(
                  F.delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(s3Object.key()).build()))
                )
                .map { resp => S3Path(bucket, s3Object.key(), new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some) }
            } else {
              S3Path(bucket, s3Object.key(), new S3MetaInfo.S3ObjectMetaInfo(s3Object).some).pure[F]
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

    Stream.eval(S3Store.liftJavaFuture(F.delay(s3.putObject(request, requestBody))).void)
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
          Pull.eval(S3Store.liftJavaFuture(F.delay(s3.putObject(request, requestBody))).void)
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
            S3Store.liftJavaFuture(F.delay(s3.createMultipartUpload(request)))

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
                    resp <- S3Store.liftJavaFuture(F.delay(s3.uploadPart(req, body)))
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
                      Pull.eval(S3Store.liftJavaFuture(F.delay(s3.completeMultipartUpload(request)))).void
                    }
              }
              .onError {
                case _ =>
                  Pull.eval(S3Store.liftJavaFuture(F.delay(s3.abortMultipartUpload(abortReq)))).void
              }
          }
      }
      .stream
}

object S3Store {
  def apply[F[_]](
    s3: S3AsyncClient,
    objectAcl: Option[ObjectCannedACL] = None,
    sseAlgorithm: Option[String] = None,
    defaultFullMetadata: Boolean = false,
    defaultTrailingSlashFiles: Boolean = false,
    bufferSize: Int = S3Store.multiUploadDefaultPartSize.toInt
  )(implicit F: ConcurrentEffect[F]): F[S3Store[F]] =
    if (bufferSize < S3Store.multiUploadMinimumPartSize) {
      new IllegalArgumentException(s"Buffer size must be at least ${S3Store.multiUploadMinimumPartSize}").raiseError
    } else {
      new S3Store(s3, objectAcl, sseAlgorithm, defaultFullMetadata, defaultTrailingSlashFiles, bufferSize).pure[F]
    }

  private def bucketKeyMetaFromPath(path: Path): Option[(String, String, Option[S3MetaInfo])] =
    S3Path.narrow(path) match {
      case None =>
        path.root.map { bucket =>
          (
            bucket,
            path.pathFromRoot
              .mkString_("/") ++ path.fileName.fold("/")((if (path.pathFromRoot.isEmpty) "" else "/") ++ _),
            None
          )
        }
      case Some(s3Path) => Some((s3Path.bucket, s3Path.key, s3Path.meta))
    }
  private def missingRootError(msg: String) =
    new IllegalArgumentException(s"$msg - root (bucket) is required to reference blobs in S3")

  private def liftJavaFuture[F[_], A](fa: F[CompletableFuture[A]])(implicit F: Concurrent[F]): F[A] = fa.flatMap { cf =>
    F.cancelable { cb =>
      cf.handle[Unit]((result: A, err: Throwable) =>
        Option(err) match {
          case None =>
            cb(Right(result))
          case Some(_: CancellationException) =>
            ()
          case Some(ex: CompletionException) =>
            cb(Left(Option(ex.getCause).getOrElse(ex)))
          case Some(ex) =>
            cb(Left(ex))
        }
      )
      F.delay(cf.cancel(true)).void
    }
  }

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
                      Pull.output1[F, CompletedPart](cp) *>
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
                .flatMap { newCompletion => hotswap.clear *> F.delay(byteBuffer.limit(currentBytes)) *> newCompletion }
            )
            .flatMap(Pull.output1)
        } else {
          Pull.done
        }
    }
  }
}
