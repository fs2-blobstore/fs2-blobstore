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

import cats.effect.{Blocker, Concurrent, ContextShift, ExitCase, Resource}
import cats.syntax.functor._
import cats.syntax.flatMap._
import fs2.{Chunk, Pipe, Stream}

import scala.jdk.CollectionConverters._
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder, Upload}

final class S3Store[F[_]](
  transferManager: TransferManager,
  objectAcl: Option[CannedAccessControlList] = None,
  sseAlgorithm: Option[String] = None,
  blocker: Blocker
)(implicit F: Concurrent[F], CS: ContextShift[F])
  extends Store[F] {

  val s3: AmazonS3 = transferManager.getAmazonS3Client

  override def list(path: Path): Stream[F, Path] = {
    def _chunk(ol: ObjectListing): Chunk[Path] = {
      val dirs: Chunk[Path] =
        Chunk.seq(
          ol.getCommonPrefixes.asScala.map(s => Path(ol.getBucketName, s.dropRight(1), None, isDir = true, None))
        )
      val files: Chunk[Path] = Chunk.seq(
        ol.getObjectSummaries.asScala.map(o =>
          Path(o.getBucketName, o.getKey, Option(o.getSize), isDir = false, Option(o.getLastModified))
        )
      )

      Chunk.concat(Seq(dirs, files))
    }
    Stream.unfoldChunkEval[F, () => Option[ObjectListing], Path] {
      // State type is a function that can provide next ObjectListing
      // initially we get it from listObjects, subsequent iterations get it from listNextBatchOfObjects
      () => Option(s3.listObjects(new ListObjectsRequest(path.root, path.key, "", Path.SEP.toString, 1000)))
    } {
      // to unfold the stream we need to emit a Chunk for the current ObjectListing received from state function
      // if returned ObjectListing is not truncated we emit the last Chunk and set up state function to return None
      // and stop unfolding in the next iteration
      getObjectListing =>
        blocker.delay {
          getObjectListing().map { ol =>
            if (ol.isTruncated)
              (_chunk(ol), () => Option(s3.listNextBatchOfObjects(ol)))
            else
              (_chunk(ol), () => None)
          }
        }
    }
  }

  override def get(path: Path, chunkSize: Int): Stream[F, Byte] = {
    val is: F[InputStream] = blocker.delay(s3.getObject(path.root, path.key).getObjectContent)
    fs2.io.readInputStream(is, chunkSize, closeAfterUse = true, blocker = blocker)
  }

  override def put(path: Path): Pipe[F, Byte, Unit] = { in =>
    val init: F[(PipedOutputStream, PipedInputStream)] = F.delay {
      val os = new PipedOutputStream()
      val is = new PipedInputStream(os)
      (os, is)
    }

    val consume: ((PipedOutputStream, PipedInputStream)) => Stream[F, Unit] = ios => {
      val putToS3 = Stream.eval(blocker.delay {
        val meta = new ObjectMetadata()
        path.size.foreach(meta.setContentLength)
        sseAlgorithm.foreach(meta.setSSEAlgorithm)
        transferManager.upload(path.root, path.key, ios._2, meta).waitForCompletion()
        objectAcl.foreach(acl => s3.setObjectAcl(path.root, path.key, acl))
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
    for {
      _ <- copy(src, dst)
      _ <- remove(src)
    } yield ()

  override def copy(src: Path, dst: Path): F[Unit] = blocker.delay {
    val meta = new ObjectMetadata()
    sseAlgorithm.foreach(meta.setSSEAlgorithm)
    val req = new CopyObjectRequest(src.root, src.key, dst.root, dst.key).withNewObjectMetadata(meta)
    transferManager.copy(req).waitForCompletion()
  }

  override def remove(path: Path): F[Unit] = blocker.delay(s3.deleteObject(path.root, path.key))

  override def putRotate(computePath: F[Path], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, (PipedOutputStream, Upload)] = for {
      p <- Resource.liftF(computePath)
      ss <- Resource.make(F.delay {
        val os = new PipedOutputStream()
        val is = new PipedInputStream(os)
        (os, is)
      }) {
        case (os, is) =>
          F.delay {
            is.close()
            os.close()
          }
      }
      upload <- Resource.makeCase(blocker.delay {
        val meta = new ObjectMetadata()
        p.size.foreach(meta.setContentLength)
        sseAlgorithm.foreach(meta.setSSEAlgorithm)
        transferManager.upload(p.root, p.key, ss._2, meta)
      }) {
        case (upload, ExitCase.Completed) =>
          blocker.delay {
            ss._1.close()
            upload.waitForCompletion()
            objectAcl.foreach(acl => s3.setObjectAcl(p.root, p.key, acl))
          }
        case (upload, _) =>
          blocker.delay {
            ss._1.close()
            upload.abort()
          }
      }
    } yield (ss._1, upload)

    putRotateBase(limit, openNewFile)(osAndUpload => bytes => blocker.delay(osAndUpload._1.write(bytes)))
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

}
