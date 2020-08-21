package blobstore

import java.nio.charset.StandardCharsets

import blobstore.url.{Path, Url}
import blobstore.url.Authority.Bucket
import blobstore.url.exception.MultipleUrlValidationException
import blobstore.url.general.GeneralFileSystemObject
import cats.{ApplicativeError, MonadError}
import cats.effect.{Blocker, ContextShift, Sync}
import cats.instances.try_._
import cats.syntax.all._
import fs2.{Pipe, Stream}

import scala.util.{Failure, Success, Try}

/**
 * Store that implements a weak API that performs runtime validation of input
 *
 * This type can be used to abstract over both FlatStore and hierarchical stores. It operates on URLs only and you
 * will get a runtime failure if the input doesn't match the expected format of the underlying store.
 *
 * For instance, if this store is backed by an SFTP store and you send it a URL to an S3 bucket, it will fail. Likewise,
 * if it's connected to one SFTP instance and you pass it a URL to another, it will also fail.
 *
 * See [[FlatStore.liftToWeak]]
 *
 * @tparam F
 */
trait NewStore[F[_]] { // We could validate schemes here as well ...

  def list(url: Url.Plain, recursive: Boolean): Stream[F, Path.GeneralObject]

  def listAll(url: Url.Plain, recursive: Boolean)(implicit F: Sync[F]): F[List[Path.GeneralObject]]

  def get[A](url: Url.Plain, chunkSize: Int): Stream[F, Byte]

  def put(url: Url.Plain, overwrite: Boolean, size: Option[Long]): Pipe[F, Byte, Unit]

  def put[A](url: Url.Plain, contents: String, overwrite: Boolean)(implicit F: Sync[F]): F[Unit] = {
    val bytes = contents.getBytes(StandardCharsets.UTF_8)
    Stream
      .emits(bytes)
      .covary[F]
      .through(put(url, overwrite, Option(bytes.size.toLong)))
      .compile
      .drain
  }

  /**
    * Write contents of src file into dst Path
    * @param src java.nio.file.Path
    * @return F[Unit]
    */
  def put[A](url: Url.Plain, src: java.nio.file.Path, blocker: Blocker, overwrite: Boolean)(
    implicit F: Sync[F],
    CS: ContextShift[F]
  ): F[Unit] =
    fs2.io.file
      .readAll(src, blocker, 4096)
      .through(put(url, overwrite = overwrite, size = Option(src.toFile.length)))
      .compile
      .drain

  /**
    * Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def move(src: Url.Plain, dst: Url.Plain): F[Unit]

  /**
    * Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def copy(src: Url.Plain, dst: Url.Plain): F[Unit]

  /**
    * Remove bytes for given path. Call should succeed even if there is nothing stored at that path.
    * @param path to remove
    * @return F[Unit]
    */
  def remove(path: Url.Plain): F[Unit]

}

object NewStore {

  private[blobstore] class StoreDelegator[F[_]: MonadError[*[_], Throwable], Blob](
    weakenBlob: Blob => GeneralFileSystemObject,
    underlying: Either[FlatStore[F, Blob], HierarchicalStore[F, Blob]]
  ) extends NewStore[F] {

    override def list(url: Url.Plain, recursive: Boolean): Stream[F, Path.GeneralObject] =
      biFlatMapLift[Stream[F, *], Path.GeneralObject](url)(
        _.list(_, recursive).map(_.map(weakenBlob)),
        _.list(_, recursive).map(_.map(weakenBlob))
      )

    override def listAll(url: Url.Plain, recursive: Boolean)(implicit F: Sync[F]): F[List[Path.GeneralObject]] =
      biFlatMapLift[F, List[Path.GeneralObject]](url)(
        _.listAll(_, recursive).map(_.map(_.map(weakenBlob))),
        _.list(_, recursive).map(_.map(weakenBlob)).compile.toList
      )

    override def get[A](url: Url.Plain, chunkSize: Int): Stream[F, Byte] =
      biFlatMapLift[Stream[F, *], Byte](url)(
        _.get(_, chunkSize),
        _.get(_, chunkSize)
      )

    override def put(url: Url.Plain, overwrite: Boolean, size: Option[Long]): Pipe[F, Byte, Unit] = s => {
      val t = biFlatMapLift[Try, Pipe[F, Byte, Unit]](url)(
        _.put(_, overwrite, size).pure[Try] ,
        _.put(_, overwrite).pure[Try]
      )

      t match {
        case Success(value) => s.through(value)
        case Failure(exception) => s.flatMap(_ => Stream.raiseError[F](exception))
      }
    }

    override def move(src: Url.Plain, dst: Url.Plain): F[Unit] =
      underlying match {
        case Left(flat) => (validateForFlat[F](src), validateForFlat[F](dst)).tupled.flatMap {
          case (s, d) => flat.move(s, d)
        }
        case Right(hier) => (validateForHierarchical[F](src, hier), validateForHierarchical[F](dst, hier)).tupled.flatMap {
          case (s, d) => hier.move(s, d)
        }
      }

    override def copy(src: Url.Plain, dst: Url.Plain): F[Unit] =
      underlying match {
        case Left(flat) => (validateForFlat[F](src), validateForFlat[F](dst)).tupled.flatMap {
          case (s, d) => flat.copy(s, d)
        }
        case Right(hier) => (validateForHierarchical[F](src, hier), validateForHierarchical[F](dst, hier)).tupled.flatMap {
          case (s, d) => hier.copy(s, d)
        }
      }

    override def remove(path: Url.Plain): F[Unit] =
      biFlatMapLift[F, Unit](path)(
        _.remove(_),
        _.remove(_)
      )

    private def validateForFlat[G[_]: ApplicativeError[*[_], Throwable]](url: Url.Plain): G[Url[Bucket]] =
      Url.forBucket(url.show).leftMap(MultipleUrlValidationException.apply).liftTo[G]

    private def validateForHierarchical[G[_]: ApplicativeError[*[_], Throwable]](
      url: Url.Plain,
      hierarchicalStore: HierarchicalStore[F, Blob]
    ): G[Path.Plain] =
      if (url.authority.equalsIgnoreUserInfo(hierarchicalStore.authority)) url.path.pure[G]
      else
        new Exception(
          show"Expected authorities to match, but got ${url.authority} for ${hierarchicalStore.authority}"
        ).raiseError[G, Path.Plain]

    private def biFlatMapLift[G[_]: MonadError[*[_], Throwable], A](url: Url.Plain)(
      f: (FlatStore[F, Blob], Url[Bucket]) => G[A],
      g: (HierarchicalStore[F, Blob], Path.Plain) => G[A]
    ): G[A] =
      underlying match {
        case Left(flatStore)          => validateForFlat[G](url).flatMap(f(flatStore, _))
        case Right(hierarchicalStore) => validateForHierarchical[G](url, hierarchicalStore).flatMap(g(hierarchicalStore, _))
      }
  }

}
