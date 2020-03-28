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

import java.io.{InputStream, PipedInputStream, PipedOutputStream}

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.syntax.all._
import cats.instances.string._
import cats.instances.option._
import cats.instances.list._
import com.amazonaws.AmazonServiceException
import fs2.{Chunk, Pipe, Stream}

import scala.jdk.CollectionConverters._
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}

final class S3Store[F[_]](
  transferManager: TransferManager,
  objectAcl: Option[CannedAccessControlList] = None,
  sseAlgorithm: Option[String] = None,
  blocker: Blocker,
  defaultFullMetadata: Boolean = false,
  defaultTrailingSlashFiles: Boolean = false
)(implicit F: Concurrent[F], CS: ContextShift[F])
  extends Store[F] {

  private val s3: AmazonS3 = transferManager.getAmazonS3Client

  override def list(path: Path): Stream[F, Path] = listUnderlying(path, defaultFullMetadata, defaultTrailingSlashFiles)

  override def get(path: Path, chunkSize: Int): Stream[F, Byte] = S3Store.pathToBucketAndKey(path) match {
    case None => Stream.raiseError(S3Store.missingRootError(s"Unable to read '$path'"))
    case Some((bucket, key)) =>
      val fis = blocker.delay(s3.getObject(bucket, key).getObjectContent: InputStream)
      fs2.io.readInputStream(fis, chunkSize, closeAfterUse = true, blocker = blocker)
  }

  override def put(path: Path): Pipe[F, Byte, Unit] =
    in =>
      S3Store.pathToBucketAndKey(path) match {
        case None => Stream.raiseError(S3Store.missingRootError(s"Unable to write to '$path'"))
        case Some((bucket, key)) =>
          val init: F[(PipedOutputStream, PipedInputStream)] = F.delay {
            val os = new PipedOutputStream()
            val is = new PipedInputStream(os)
            (os, is)
          }
          val consume: ((PipedOutputStream, PipedInputStream)) => Stream[F, Unit] = ios => {
            val putToS3 = Stream.eval(blocker.delay {
              val req: PutObjectRequest = S3Store.pathToPutObjectRequest(bucket, key, sseAlgorithm, ios._2)(path)
              transferManager.upload(req).waitForCompletion()
              objectAcl.foreach(acl => s3.setObjectAcl(bucket, key, acl))
              ()
            })
            val writeBytes: Stream[F, Unit] =
              _writeAllToOutputStream1(in, ios._1, blocker).stream ++ Stream.eval(F.delay(ios._1.close()))

            putToS3 concurrently writeBytes
          }
          val release: ((PipedOutputStream, PipedInputStream)) => F[Unit] = ios =>
            F.delay {
              ios._2.close()
              ios._1.close()
            }

          Stream.bracket(init)(release).flatMap(consume)
      }

  override def move(src: Path, dst: Path): F[Unit] =
    copy(src, dst) *> remove(src)

  override def copy(src: Path, dst: Path): F[Unit] =
    for {
      (srcBucket, srcKey) <- S3Store
        .pathToBucketAndKey(src)
        .fold[F[(String, String)]](S3Store.missingRootError(s"Wrong src '$src'").raiseError)(_.pure[F])
      (dstBucket, dstKey) <- S3Store
        .pathToBucketAndKey(dst)
        .fold[F[(String, String)]](S3Store.missingRootError(s"Wrong dst '$dst'").raiseError)(_.pure[F])
      _ <- blocker.delay {
        val meta = new ObjectMetadata()
        sseAlgorithm.foreach(meta.setSSEAlgorithm)
        val req = new CopyObjectRequest(srcBucket, srcKey, dstBucket, dstKey).withNewObjectMetadata(meta)
        transferManager.copy(req).waitForCompletion()
      }
    } yield ()

  override def remove(path: Path): F[Unit] = S3Store.pathToBucketAndKey(path) match {
    case Some((bucket, key)) =>
      blocker.delay(s3.deleteObject(bucket, key))
    case None => S3Store.missingRootError(s"Unable to remove '$path'").raiseError[F, Unit]
  }

  def listUnderlying(path: Path, fullMetadata: Boolean, expectTrailingSlashFiles: Boolean): Stream[F, S3Path] =
    S3Store.pathToBucketAndKey(path) match {
      case None => Stream.raiseError(S3Store.missingRootError(s"Unable to list '$path'"))
      case Some((bucket, key)) =>
        val listObjectsRequest = new ListObjectsV2Request()
          .withBucketName(bucket)
          .withPrefix(if (key == "/") "" else key)
          .withDelimiter("/")
          .withMaxKeys(1000)
        Stream.unfoldChunkEval[F, () => Option[ListObjectsV2Result], S3Path] { () =>
          Option(s3.listObjectsV2(listObjectsRequest))
        } { getObjectListing =>
          blocker.delay(getObjectListing()).flatMap {
            case None => none[(Chunk[S3Path], () => Option[ListObjectsV2Result])].pure[F]
            case Some(ol) =>
              val fDirs = ol.getCommonPrefixes.asScala.toList.traverse { prefix =>
                if (expectTrailingSlashFiles) {
                  blocker
                    .delay(s3.getObjectMetadata(bucket, prefix))
                    .map { objectMetadata =>
                      new S3Path(
                        S3Path
                          .S3File(
                            S3Path.S3File.summaryFromMetadata(bucket, prefix, objectMetadata),
                            if (fullMetadata) objectMetadata.some
                            else none
                          )
                          .asRight,
                        knownSize = true
                      )
                    }
                    .recover {
                      case e: AmazonServiceException if e.getStatusCode == 404 =>
                        new S3Path(S3Path.S3Dir(bucket, prefix).asLeft, knownSize = true)
                    }
                } else {
                  new S3Path(S3Path.S3Dir(bucket, prefix).asLeft, knownSize = true).pure[F]
                }
              }
              val fFiles = ol.getObjectSummaries.asScala.toList.traverse { objectSummary =>
                val fFile = if (fullMetadata) {
                  blocker
                    .delay(s3.getObjectMetadata(objectSummary.getBucketName, objectSummary.getKey))
                    .map(objectMetadata => S3Path.S3File(objectSummary, objectMetadata.some))
                } else {
                  S3Path.S3File(objectSummary, none).pure[F]
                }
                fFile.map(file => new S3Path(file.asRight, knownSize = true))
              }
              (fDirs, fFiles).mapN { (dirs, files) =>
                (
                  Chunk.concat(Seq(Chunk.seq(dirs), Chunk.seq(files))),
                  () =>
                    if (ol.isTruncated)
                      Option(s3.listObjectsV2(listObjectsRequest.withContinuationToken(ol.getContinuationToken)))
                    else none
                ).some
              }
          }
        }
    }
}

object S3Store {

  /**
    * Safely initialize S3Store and shutdown Amazon S3 client upon finish.
    *
    * @param transferManager  F[TransferManager] how to connect AWS S3 client
    * @param encrypt Boolean true to force all writes to use SSE algorithm ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION
    * @return Stream[ F, S3Store[F] ] stream with one S3Store, AmazonS3 client will disconnect once stream is done.
    */
  def apply[F[_]: ContextShift](transferManager: F[TransferManager], encrypt: Boolean, blocker: Blocker)(
    implicit F: Concurrent[F]
  ): Stream[F, S3Store[F]] = {
    val opt = if (encrypt) Option(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION) else None
    apply(transferManager, None, opt, blocker)
  }

  /**
    * Safely initialize S3Store using TransferManagerBuilder.standard() and shutdown client upon finish.
    *
    * NOTICE: Standard S3 client builder uses the Default Credential Provider Chain, see docs on how to authenticate:
    *         https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
    *
    * @param encrypt Boolean true to force all writes to use SSE algorithm ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION
    * @return Stream[ F, S3Store[F] ] stream with one S3Store, AmazonS3 client will disconnect once stream is done.
    */
  def apply[F[_]: ContextShift](encrypt: Boolean, blocker: Blocker)(
    implicit F: Concurrent[F]
  ): Stream[F, S3Store[F]] = {
    val opt = if (encrypt) Option(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION) else None
    apply(F.delay(TransferManagerBuilder.standard().build()), None, opt, blocker)
  }

  /**
    * Safely initialize S3Store using TransferManagerBuilder.standard() and shutdown client upon finish.
    *
    * NOTICE: Standard S3 client builder uses the Default Credential Provider Chain, see docs on how to authenticate:
    *         https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
    *
    * @param sseAlgorithm SSE algorithm, ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION is recommended.
    * @return Stream[ F, S3Store[F] ] stream with one S3Store, AmazonS3 client will disconnect once stream is done.
    */
  def apply[F[_]: ContextShift](sseAlgorithm: String, blocker: Blocker)(
    implicit F: Concurrent[F]
  ): Stream[F, S3Store[F]] =
    apply(F.delay(TransferManagerBuilder.standard().build()), None, Option(sseAlgorithm), blocker)

  /**
    * Safely initialize S3Store using TransferManagerBuilder.standard() and shutdown client upon finish.
    *
    * NOTICE: Standard S3 client builder uses the Default Credential Provider Chain, see docs on how to authenticate:
    *         https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
    *
    * @return Stream[ F, S3Store[F] ] stream with one S3Store, AmazonS3 client will disconnect once stream is done.
    */
  def apply[F[_]: ContextShift](blocker: Blocker)(implicit F: Concurrent[F]): Stream[F, S3Store[F]] =
    apply(F.delay(TransferManagerBuilder.standard().build()), None, None, blocker)

  /**
    * Safely initialize S3Store and shutdown Amazon S3 client upon finish.
    *
    * @param transferManager F[TransferManager] how to connect AWS S3 client
    * @param sseAlgorithm Option[String] Server Side Encryption algorithm
    * @return Stream[ F, S3Store[F] ] stream with one S3Store, AmazonS3 client will disconnect once stream is done.
    */
  def apply[F[_]](transferManager: F[TransferManager], sseAlgorithm: Option[String], blocker: Blocker)(
    implicit F: Concurrent[F],
    CS: ContextShift[F]
  ): Stream[F, S3Store[F]] =
    Stream
      .bracket(transferManager)(tm => F.delay(tm.shutdownNow()))
      .map(tm => new S3Store[F](tm, None, sseAlgorithm, blocker))

  /**
    * Safely initialize S3Store and shutdown Amazon S3 client upon finish.
    * @param transferManager F[TransferManager] how to connect AWS S3 client
    * @param objectAcl Option[CannedAccessControlList] ACL that all uploaded objects should have
    * @param sseAlgorithm Option[String] Server Side Encryption algorithm
    * @return Stream[ F, S3Store[F] ] stream with one S3Store, AmazonS3 client will disconnect once stream is done.
    */
  def apply[F[_]](
    transferManager: F[TransferManager],
    objectAcl: Option[CannedAccessControlList],
    sseAlgorithm: Option[String],
    blocker: Blocker
  )(implicit F: Concurrent[F], CS: ContextShift[F]): Stream[F, S3Store[F]] =
    Stream
      .bracket(transferManager)(tm => F.delay(tm.shutdownNow()))
      .map(tm => new S3Store[F](tm, objectAcl, sseAlgorithm, blocker))

  private def pathToBucketAndKey(path: Path): Option[(String, String)] =
    S3Path
      .narrow(path)
      .fold(
        path.root.map(bucket =>
          (
            bucket,
            path.pathFromRoot
              .mkString_("/") ++ path.fileName.fold("/")((if (path.pathFromRoot.isEmpty) "" else "/") ++ _)
          )
        )
      )(
        _.s3Entity.fold(
          dir => (dir.bucket, dir.path.mkString_("", "/", "/")).some,
          file => (Option(file.summary.getBucketName), Option(file.summary.getKey)).tupled
        )
      )

  private def missingRootError(msg: String) =
    new IllegalArgumentException(s"$msg - root (bucket) is required to reference blobs in S3")

  private def pathToPutObjectRequest(
    bucket: String,
    key: String,
    sseAlgorithm: Option[String],
    inputStream: InputStream
  )(path: Path): PutObjectRequest =
    S3Path
      .narrow(path)
      .flatMap {
        case S3Path(Right(s3File)) =>
          s3File.metadata.map { objectMeta =>
            val req = new PutObjectRequest(bucket, key, inputStream, objectMeta)
            Option(s3File.summary.getStorageClass).foreach(req.setStorageClass)
            req
          }
        case _ => none
      }
      .getOrElse {
        val m = new ObjectMetadata()
        sseAlgorithm.foreach(m.setSSEAlgorithm)
        path.size.foreach(m.setContentLength)
        new PutObjectRequest(bucket, key, inputStream, m)
      }
}
