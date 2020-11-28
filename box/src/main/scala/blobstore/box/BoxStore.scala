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

import java.io.{InputStream, OutputStream, PipedInputStream, PipedOutputStream}

import blobstore.url.{Authority, FileSystemObject, Path, Url}
import cats.data.Validated
import cats.effect.{Blocker, Concurrent, ContextShift, ExitCase, Resource}
import cats.syntax.all._
import com.box.sdk.{BoxAPIConnection, BoxFile, BoxFolder, BoxItem, BoxResource}
import fs2.{Pipe, Stream}

import scala.jdk.CollectionConverters._

class BoxStore[F[_]](
  api: BoxAPIConnection,
  blocker: Blocker,
  rootFolderId: String,
  largeFileThreshold: Long = 50L * 1024L * 1024L
)(
  implicit F: Concurrent[F],
  CS: ContextShift[F]
) extends PathStore[F, BoxPath] {

  private val rootFolder = new BoxFolder(api, rootFolderId)

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[BoxPath]] =
    listUnderlying(path, Array.empty, recursive)

  override def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte] = {
    val init: F[(OutputStream, InputStream)] = blocker.delay {
      val is = new PipedInputStream()
      val os = new PipedOutputStream(is)
      (os, is)
    }
    val release: ((OutputStream, InputStream)) => F[Unit] = ios =>
      blocker.delay {
        ios._1.close()
        ios._2.close()
      }

    def consume(file: BoxFile)(streams: (OutputStream, InputStream)): Stream[F, Byte] = {
      val dl = Stream.eval(blocker.delay {
        file.download(streams._1)
        streams._1.close()
      })
      val readInput = fs2.io.readInputStream(F.delay(streams._2), chunkSize, closeAfterUse = true, blocker = blocker)
      readInput concurrently dl
    }

    Stream.eval(boxFileAtPath(path)).flatMap {
      case None =>
        Stream.raiseError(new IllegalArgumentException(s"Item at path '$path' doesn't exist or is not a File"))
      case Some(file) => Stream.bracket(init)(release).flatMap(consume(file))
    }
  }

  override def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = {
    in =>
      path.lastSegment match {
        case None =>
          Stream.raiseError(new IllegalArgumentException(s"Specified path '$path' doesn't point to a file."))
        case Some(name) =>
          val init: F[(OutputStream, InputStream, Either[BoxFile, BoxFolder])] = {
            val os = new PipedOutputStream()
            val is = new PipedInputStream(os)
            boxFileAtPath(path).flatMap {
              case Some(existing) if overwrite =>
                (os: OutputStream, is: InputStream, existing.asLeft[BoxFolder]).pure[F]
              case Some(_) =>
                F.raiseError(new IllegalArgumentException(s"File at path '$path' already exist."))
              case None =>
                putFolderAtPath(rootFolder, path.up.segments.toList).map { parentFolder =>
                  (os: OutputStream, is: InputStream, parentFolder.asRight[BoxFile])
                }
            }
          }

          val consume: ((OutputStream, InputStream, Either[BoxFile, BoxFolder])) => Stream[F, Unit] = ios => {
            val putToBox = Stream.eval(blocker.delay {
              size match {
                case Some(size) if size > largeFileThreshold =>
                  ios._3.fold(_.uploadLargeFile(ios._2, size), _.uploadLargeFile(ios._2, name, size))
                case _ =>
                  ios._3.fold(_.uploadNewVersion(ios._2), _.uploadFile(ios._2, name))
              }
            }.void)

            val writeBytes =
              _writeAllToOutputStream1(in, ios._1, blocker).stream ++ Stream.eval(F.delay(ios._1.close()))
            putToBox concurrently writeBytes
          }

          val release: ((OutputStream, InputStream, Either[BoxFile, BoxFolder])) => F[Unit] = ios =>
            blocker.delay {
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
          blocker.delay(file.move(
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
          blocker.delay(file.copy(
            folder,
            dst.lastSegment.filter(s => !s.endsWith("/")).getOrElse(file.getInfo.getName)
          )).void
        )
    case None => ().pure[F]
  }

  override def remove[A](path: Path[A], recursive: Boolean): F[Unit] =
    boxFileAtPath(path).flatMap {
      case Some(bf) => blocker.delay(bf.delete())
      case None     => ().pure
    }

  override def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, OutputStream] = for {
      p <- Resource.liftF(computePath)
      name <- Resource.liftF(
        F.fromEither(p.lastSegment.filter(s => !s.endsWith("/")).toRight(new IllegalArgumentException(
          s"Specified path '$p' doesn't point to a file."
        )))
      )
      fileOrFolder <- Resource.liftF(boxFileAtPath(p).flatMap {
        case None       => putFolderAtPath(rootFolder, p.up.segments.toList).map(_.asRight[BoxFile])
        case Some(file) => file.asLeft[BoxFolder].pure[F]
      })
      (os, is) <- Resource.make(F.delay {
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
      _ <- Resource.makeCase(F.unit) {
        case (_, ExitCase.Completed) =>
          blocker.delay {
            os.close()
            fileOrFolder.fold(_.uploadNewVersion(is), _.uploadFile(is, name))
            ()
          }
        case (_, _) =>
          F.unit
      }
    } yield os

    putRotateBase(limit, openNewFile)(os => bytes => blocker.delay(os.write(bytes.toArray)))
  }

  @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
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
                blocker,
                BoxStore.blockingIterator(new BoxFolder(api, f.getID), fields)
              )
              .map {
                case i if BoxStore.isFile(i)   => i.asInstanceOf[BoxFile#Info].asLeft.some
                case i if BoxStore.isFolder(i) => i.asInstanceOf[BoxFolder#Info].asRight.some
                case _                         => none
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
            if (BoxStore.isFile(info)) path.as(BoxPath(info.asInstanceOf[BoxFile#Info].asLeft)).some // scalafix:ok
            else if (BoxStore.isFolder(info))
              path.as(BoxPath(info.asInstanceOf[BoxFolder#Info].asRight)).some // scalafix:ok
            else none[Path[BoxPath]]
          case None => None
        }
      case someInfo => someInfo.pure
    }

  private def boxInfoAtPathStream[A](
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
          .fromBlockingIterator(blocker, BoxStore.blockingIterator(parentFolder, fields))
          .find(_.getName.equalsIgnoreCase(head))
      case head :: tail =>
        Stream
          .fromBlockingIterator(blocker, BoxStore.blockingIterator(parentFolder, Array.empty))
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
        .fromBlockingIterator(blocker, BoxStore.blockingIterator(parentFolder, Array.empty))
        .find(_.getName.equalsIgnoreCase(head))
        .compile
        .last
        .flatMap[BoxFolder] {
          case Some(info) if BoxStore.isFolder(info) =>
            new BoxFolder(api, info.getID).pure[F]
          case Some(_) =>
            F.raiseError(
              new IllegalArgumentException(
                s"Can't create Folder '$head' along path - File with this name already exists"
              )
            )
          case None =>
            blocker.delay(parentFolder.createFolder(head).getResource)
        }
        .flatMap(putFolderAtPath(_, tail))
  }

  /** Lifts this FileStore to a Store accepting URLs with authority `A` and exposing blobs of type `B`. You must provide
    * a mapping from this Store's BlobType to B, and you may provide a function `g` for controlling input paths to this store.
    *
    * Input URLs to the returned store are validated against this Store's authority before the path is extracted and passed
    * to this store.
    */
  override def liftTo[A <: Authority, B](
    f: BoxPath => B,
    g: Url[A] => Validated[Throwable, Path.Plain]
  ): Store[F, A, B] =
    new Store.DelegatingStore[F, BoxPath, A, B](f, Right(this), g)

  override def transferTo[A <: Authority, B, P](dstStore: Store[F, A, B], srcPath: Path[P], dstUrl: Url[A])(implicit
  fsb: FileSystemObject[B]): F[Int] = ???

  override def getContents[A](path: Path[A], chunkSize: Int): F[String] =
    get(path, chunkSize).through(fs2.text.utf8Decode).compile.string
}

object BoxStore {
  def apply[F[_]](
    api: BoxAPIConnection,
    blocker: Blocker,
    rootFolderId: String = RootFolderId
  )(implicit F: Concurrent[F], CS: ContextShift[F]): BoxStore[F] = new BoxStore(api, blocker, rootFolderId)

  val RootFolderId = "0"

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
    folder.getChildren((fields ++ requiredFields).distinct: _*).iterator().asScala

  private def isFile(info: BoxItem#Info): Boolean = info.getType == BoxStore.fileResourceType

  private def isFolder(info: BoxItem#Info): Boolean = info.getType == BoxStore.folderResourceType

  private def lub(either: Either[BoxItem#Info, BoxItem#Info]): BoxItem#Info = either.fold(identity, identity)

}
