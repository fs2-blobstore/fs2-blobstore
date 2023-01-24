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
package box

import blobstore.url.exception.Throwables
import blobstore.url.{FsObject, Path, Url}
import cats.data.{Validated, ValidatedNec}
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.box.sdk.{BoxAPIConnection, BoxFile, BoxFolder, BoxItem, BoxResource}
import fs2.{Pipe, Stream}

import java.io.{InputStream, OutputStream, PipedInputStream, PipedOutputStream}
import scala.jdk.CollectionConverters.*

/** @param api
  *   underlying configured BoxAPIConnection
  * @param rootFolderId
  *   Root Folder Id, default – "0"
  * @param largeFileThreshold
  *   override for the threshold on the file size to be considered "large", default – 50MiB
  */
class BoxStore[F[_]: Async](
  api: BoxAPIConnection,
  rootFolderId: String,
  largeFileThreshold: Long
) extends PathStore[F, BoxPath] {

  private val rootFolder = new BoxFolder(api, rootFolderId)

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[BoxPath]] =
    listUnderlying(path, Array.empty, recursive)

  override def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte] = {
    val init: F[(OutputStream, InputStream)] = Async[F].blocking {
      val is = new PipedInputStream()
      val os = new PipedOutputStream(is)
      (os, is)
    }
    val release: ((OutputStream, InputStream)) => F[Unit] = ios =>
      Async[F].blocking {
        ios._1.close()
        ios._2.close()
      }

    def consume(file: BoxFile)(streams: (OutputStream, InputStream)): Stream[F, Byte] = {
      val dl = Stream.eval(Async[F].blocking {
        file.download(streams._1)
        streams._1.close()
      })
      val readInput = fs2.io.readInputStream(Async[F].delay(streams._2), chunkSize, closeAfterUse = true)
      readInput.concurrently(dl)
    }

    Stream.eval(boxFileAtPath(path)).flatMap {
      case None =>
        Stream.raiseError(new IllegalArgumentException(show"Item at path '$path' doesn't exist or is not a File"))
      case Some(file) => Stream.bracket(init)(release).flatMap(consume(file))
    }
  }

  override def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = {
    in =>
      path.lastSegment match {
        case None =>
          Stream.raiseError(new IllegalArgumentException(show"Specified path '$path' doesn't point to a file."))
        case Some(name) =>
          val init: F[(OutputStream, InputStream, Either[BoxFile, BoxFolder])] = {
            val os = new PipedOutputStream()
            val is = new PipedInputStream(os)
            boxFileAtPath(path).flatMap {
              case Some(existing) if overwrite =>
                (os: OutputStream, is: InputStream, existing.asLeft[BoxFolder]).pure[F]
              case Some(_) =>
                Async[F].raiseError(new IllegalArgumentException(show"File at path '$path' already exist."))
              case None =>
                putFolderAtPath(rootFolder, path.up.segments.toList).map { parentFolder =>
                  (os: OutputStream, is: InputStream, parentFolder.asRight[BoxFile])
                }
            }
          }

          val consume: ((OutputStream, InputStream, Either[BoxFile, BoxFolder])) => Stream[F, Unit] = ios => {
            val putToBox = Stream.eval(Async[F].blocking {
              size match {
                case Some(size) if size > largeFileThreshold =>
                  ios._3.fold(_.uploadLargeFile(ios._2, size), _.uploadLargeFile(ios._2, name, size))
                case _ =>
                  ios._3.fold(_.uploadNewVersion(ios._2), _.uploadFile(ios._2, name))
              }
            }.void)

            val writeBytes = {
              in.through(fs2.io.writeOutputStream(ios._1.pure, closeAfterUse = false)) ++
                Stream.eval(Async[F].delay(ios._1.close()))
            }
            putToBox.concurrently(writeBytes)
          }

          val release: ((OutputStream, InputStream, Either[BoxFile, BoxFolder])) => F[Unit] = ios =>
            Async[F].blocking {
              ios._2.close()
              ios._1.close()
            }

          Stream.bracket(init)(release).flatMap(consume)
      }
  }

  override def move[A, B](src: Path[A], dst: Path[B]): F[Unit] = boxFileAtPath(src).flatMap {
    case Some(file) =>
      putFolderAtPath(rootFolder, dst.up.segments.toList)
        .flatMap(folder =>
          Async[F].blocking(file.move(
            folder,
            dst.lastSegment.filter(s => !s.endsWith("/")).getOrElse(file.getInfo.getName)
          )).void
        )
    case None => ().pure[F]
  }

  override def copy[A, B](src: Path[A], dst: Path[B]): F[Unit] = boxFileAtPath(src).flatMap {
    case Some(file) =>
      putFolderAtPath(rootFolder, dst.up.segments.toList)
        .flatMap(folder =>
          Async[F].blocking(file.copy(
            folder,
            dst.lastSegment.filter(s => !s.endsWith("/")).getOrElse(file.getInfo.getName)
          )).void
        )
    case None => ().pure[F]
  }

  override def remove[A](path: Path[A], recursive: Boolean): F[Unit] =
    boxInfoAtPath(path).flatMap {
      case Some(p) => p.representation.fileOrFolder.fold(
          file => Async[F].blocking(new BoxFile(api, file.getID).delete()),
          folder => Async[F].blocking(new BoxFolder(api, folder.getID).delete(recursive))
        )
      case None => ().pure
    }

  override def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, OutputStream] = for {
      p <- Resource.eval(computePath)
      name <- Resource.eval(
        Async[F].fromEither(p.lastSegment.filter(s => !s.endsWith("/")).toRight(new IllegalArgumentException(
          show"Specified path '$p' doesn't point to a file."
        )))
      )
      fileOrFolder <- Resource.eval(boxFileAtPath(p).flatMap {
        case None       => putFolderAtPath(rootFolder, p.up.segments.toList).map(_.asRight[BoxFile])
        case Some(file) => file.asLeft[BoxFolder].pure[F]
      })
      (os, is) <- Resource.make(Async[F].delay {
        val os = new PipedOutputStream()
        val is = new PipedInputStream(os)
        (os, is)
      }) {
        case (os, is) =>
          Async[F].delay {
            is.close()
            os.close()
          }
      }
      _ <- Resource.makeCase(Async[F].unit) {
        case (_, Resource.ExitCase.Succeeded) =>
          Async[F].blocking {
            os.close()
            val _ = fileOrFolder.fold(_.uploadNewVersion(is), _.uploadFile(is, name))
            ()
          }
        case (_, _) =>
          Async[F].unit
      }
    } yield os

    putRotateBase(limit, openNewFile)(os => bytes => Async[F].blocking(os.write(bytes.toArray)))
  }

  def listUnderlying[A](
    path: Path[A],
    fields: Array[String] = Array.empty,
    recursive: Boolean
  ): Stream[F, Path[BoxPath]] = {
    val stream: Stream[F, Path[BoxPath]] = Stream.eval(boxInfoAtPath(path, fields)).flatMap {
      case Some(info) =>
        info.representation.lub match {
          case f if BoxStore.isFile(f) =>
            Stream.emit(info)
          case f if BoxStore.isFolder(f) =>
            Stream
              .fromBlockingIterator(
                BoxStore.blockingIterator(new BoxFolder(api, f.getID), fields),
                64
              )
              .map {
                // scalafix:off
                case i if BoxStore.isFile(i)   => i.asInstanceOf[BoxFile#Info].asLeft.some
                case i if BoxStore.isFolder(i) => i.asInstanceOf[BoxFolder#Info].asRight.some
                case _                         => none
                // scalafix:on
              }
              .unNone
              .map { fileOrFolder =>
                val lub = BoxStore.lub(fileOrFolder)
                val pathList = lub.getPathCollection.asScala.toList.tail.map(_.getName) match {
                  case "All Files" :: t => t
                  case list             => list
                }
                val path = Path(pathList.mkString("/")) / lub.getName
                path.as(BoxPath(fileOrFolder))
              }
          case _ => Stream.empty
        }
      case None => Stream.empty
    }
    if (recursive) {
      stream.flatMap {
        case p if p.isDir =>
          listUnderlying(p, fields, recursive)
        case p =>
          Stream.emit(p)
      }
    } else {
      stream
    }
  }

  override def stat[A](path: Path[A]): F[Option[Path[BoxPath]]] =
    boxInfoAtPath(path)

  private def boxFileAtPath[A](path: Path[A]): F[Option[BoxFile]] =
    boxInfoAtPath(path).map(_.flatMap(_.representation.file).map(file => new BoxFile(api, file.getID)))

  private def boxInfoAtPath[A](path: Path[A], fields: Array[String] = Array.empty): F[Option[Path[BoxPath]]] =
    BoxPath.narrow(path) match {
      case None =>
        boxInfoAtPathStream(rootFolder, path.stripSlashSuffix.segments.toList, fields).compile.last.map {
          case Some(info) =>
            // scalafix:off
            if (BoxStore.isFile(info)) path.as(BoxPath(info.asInstanceOf[BoxFile#Info].asLeft)).some
            else if (BoxStore.isFolder(info))
              path.as(BoxPath(info.asInstanceOf[BoxFolder#Info].asRight)).some
            else none[Path[BoxPath]]
          // scalafix:on
          case None => None
        }
      case someInfo => someInfo.pure
    }

  private def boxInfoAtPathStream(
    parentFolder: BoxFolder,
    pathParts: List[String],
    fields: Array[String]
  ): Stream[F, BoxItem#Info] =
    pathParts match {
      case Nil =>
        if (parentFolder == rootFolder) Stream.emit(rootFolder.getInfo).covary[F]
        else Stream.empty
      case head :: Nil =>
        Stream
          .fromBlockingIterator(BoxStore.blockingIterator(parentFolder, fields), 64)
          .find(_.getName.equalsIgnoreCase(head))
      case head :: tail =>
        Stream
          .fromBlockingIterator(BoxStore.blockingIterator(parentFolder, Array.empty), 64)
          .find { info =>
            BoxStore.isFolder(info) && info.getName.equalsIgnoreCase(head)
          }
          .flatMap { info =>
            boxInfoAtPathStream(new BoxFolder(api, info.getID), tail, fields)
          }
    }

  private def putFolderAtPath(parentFolder: BoxFolder, parts: List[String]): F[BoxFolder] = parts match {
    case Nil => parentFolder.pure[F]
    case head :: tail =>
      Stream
        .fromBlockingIterator(BoxStore.blockingIterator(parentFolder, Array.empty), 64)
        .find(_.getName.equalsIgnoreCase(head))
        .compile
        .last
        .flatMap[BoxFolder] {
          case Some(info) if BoxStore.isFolder(info) =>
            new BoxFolder(api, info.getID).pure[F]
          case Some(_) =>
            Async[F].raiseError(
              new IllegalArgumentException(
                show"Can't create Folder '$head' along path - File with this name already exists"
              )
            )
          case None =>
            Async[F].blocking(parentFolder.createFolder(head).getResource)
        }
        .flatMap(putFolderAtPath(_, tail))
  }

  /** Lifts this FileStore to a Store accepting URLs with authority `A` and exposing blobs of type `B`. You must provide
    * a mapping from this Store's BlobType to B, and you may provide a function `g` for controlling input paths to this
    * store.
    *
    * Input URLs to the returned store are validated against this Store's authority before the path is extracted and
    * passed to this store.
    */
  override def lift(g: Url.Plain => Validated[Throwable, Path.Plain]): Store[F, BoxPath] =
    new Store.DelegatingStore[F, BoxPath](this, g)

  override def transferTo[B, P, A](dstStore: Store[F, B], srcPath: Path[P], dstUrl: Url[A])(implicit
  ev: B <:< FsObject): F[Int] = defaultTransferTo(this, dstStore, srcPath, dstUrl)

  override def getContents[A](path: Path[A], chunkSize: Int): F[String] =
    get(path, chunkSize).through(fs2.text.utf8.decode).compile.string
}

object BoxStore {

  def builder[F[_]: Async](boxApiConnection: BoxAPIConnection): BoxStoreBuilder[F] =
    BoxStoreBuilderImpl[F](boxApiConnection)

  /** @see
    *   [[BoxStore]]
    */
  trait BoxStoreBuilder[F[_]] {
    def withBoxApiConnection(boxApiConnection: BoxAPIConnection): BoxStoreBuilder[F]
    def withRootFolderId(rootFolderId: String): BoxStoreBuilder[F]
    def withLargeFileThreshold(largeFileThreshold: Long): BoxStoreBuilder[F]
    def build: ValidatedNec[Throwable, BoxStore[F]]
    def unsafe: BoxStore[F] = build match {
      case Validated.Valid(a)    => a
      case Validated.Invalid(es) => throw es.reduce(Throwables.collapsingSemigroup) // scalafix:ok
    }
  }

  case class BoxStoreBuilderImpl[F[_]: Async](
    _boxApiConnection: BoxAPIConnection,
    _rootFolderId: String = "0",
    _largeFileThreshold: Long = 50L * 1024L * 1024L
  ) extends BoxStoreBuilder[F] {

    def withBoxApiConnection(boxApiConnection: BoxAPIConnection): BoxStoreBuilder[F] =
      this.copy(_boxApiConnection = boxApiConnection)

    def withRootFolderId(rootFolderId: String): BoxStoreBuilder[F] = this.copy(_rootFolderId = rootFolderId)

    def withLargeFileThreshold(largeFileThreshold: Long): BoxStoreBuilder[F] =
      this.copy(_largeFileThreshold = largeFileThreshold)

    def build: ValidatedNec[Throwable, BoxStore[F]] = {
      val validateLFT =
        if (_largeFileThreshold < 1024L * 1024L) {
          new IllegalArgumentException("Please consider increasing largeFileThreshold to at least 1MB.").invalidNec
        } else ().validNec
      validateLFT.map(_ => new BoxStore(_boxApiConnection, _rootFolderId, _largeFileThreshold))
    }
  }

  private val fileResourceType: String   = BoxResource.getResourceType(classOf[BoxFile])
  private val folderResourceType: String = BoxResource.getResourceType(classOf[BoxFolder])

  private val requiredFields = Array(
    "type",
    "id",
    "name",
    "size",
    "modified_at",
    "path_collection"
  )

  private def blockingIterator(folder: BoxFolder, fields: Array[String]): Iterator[BoxItem#Info] =
    folder.getChildren((fields ++ requiredFields).distinct*).iterator().asScala

  private def isFile(info: BoxItem#Info): Boolean = info.getType == BoxStore.fileResourceType

  private def isFolder(info: BoxItem#Info): Boolean = info.getType == BoxStore.folderResourceType

  private def lub(either: Either[BoxItem#Info, BoxItem#Info]): BoxItem#Info = either.fold(identity, identity)

}
