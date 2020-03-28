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

import cats.syntax.all._
import cats.effect.{Blocker, Concurrent, ContextShift}
import com.box.sdk.{BoxAPIConnection, BoxFile, BoxFolder, BoxItem, BoxResource}
import fs2.{Pipe, Stream}

import scala.jdk.CollectionConverters._

final class BoxStore[F[_]](
  api: BoxAPIConnection,
  rootFolderId: String,
  blocker: Blocker,
  largeFileThreshold: Long = 50L * 1024L * 1024L
)(
  implicit F: Concurrent[F],
  CS: ContextShift[F]
) extends Store[F] {
  private val rootFolder = new BoxFolder(api, rootFolderId)

  override def list(path: Path): Stream[F, Path] = listUnderlying(path, Array.empty)

  override def get(path: Path, chunkSize: Int): Stream[F, Byte] = {
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

  override def put(path: Path): Pipe[F, Byte, Unit] = { in =>
    path.fileName match {
      case None =>
        Stream.raiseError(new IllegalArgumentException(s"Specified path '$path' doesn't point to a file."))
      case Some(name) =>
        val init: F[(OutputStream, InputStream, BoxFolder)] =
          putFolderAtPath(rootFolder, BoxPath.parentPath(path)).map { parentFolder =>
            val os = new PipedOutputStream()
            val is = new PipedInputStream(os)
            (os, is, parentFolder)
          }

        val consume: ((OutputStream, InputStream, BoxFolder)) => Stream[F, Unit] = ios => {
          val putToBox = Stream.eval(blocker.delay {
            path.size match {
              case Some(size) if size > largeFileThreshold => ios._3.uploadLargeFile(ios._2, name, size)
              case _                                       => ios._3.uploadFile(ios._2, name)
            }
          }.void)

          val writeBytes = _writeAllToOutputStream1(in, ios._1, blocker).stream ++ Stream.eval(F.delay(ios._1.close()))
          putToBox concurrently writeBytes
        }

        val release: ((OutputStream, InputStream, BoxFolder)) => F[Unit] = ios =>
          blocker.delay {
            ios._2.close()
            ios._1.close()
          }

        Stream.bracket(init)(release).flatMap(consume)
    }
  }

  override def move(src: Path, dst: Path): F[Unit] = boxFileAtPath(src).flatMap {
    case Some(file) =>
      putFolderAtPath(rootFolder, BoxPath.parentPath(dst))
        .flatMap(folder => blocker.delay(file.move(folder, dst.fileName.getOrElse(file.getInfo.getName))).void)
    case None => ().pure[F]
  }

  override def copy(src: Path, dst: Path): F[Unit] = boxFileAtPath(src).flatMap {
    case Some(file) =>
      putFolderAtPath(rootFolder, BoxPath.parentPath(dst))
        .flatMap(folder => blocker.delay(file.copy(folder, dst.fileName.getOrElse(file.getInfo.getName))).void)
    case None => ().pure[F]
  }

  override def remove(path: Path): F[Unit] =
    boxFileAtPath(path).flatMap {
      case Some(bf) => blocker.delay(bf.delete())
      case None     => ().pure
    }

  @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
  def listUnderlying(path: Path, fields: Array[String] = Array.empty): Stream[F, BoxPath] = {
    def getRoot: Stream[F, (Option[String], Option[String])] =
      BoxPath
        .narrow(path)
        .fold {
          path.root.fold(Stream.emit(none[String] -> none[String]).covary[F]) { r =>
            boxInfoAtPathStream(rootFolder, r :: Nil, Array.empty).head.flatMap {
              case info if BoxStore.isFolder(info) =>
                Stream.emit(r.some -> info.getID.some)
            }
          }
        }(bp => Stream.emit(bp.root -> bp.rootId).covary[F])

    Stream.eval(boxInfoAtPath(path, fields)).flatMap {
      case Some(info) =>
        getRoot.flatMap {
          case (root, rootInfo) =>
            info match {
              case _ if BoxStore.isFile(info) =>
                Stream.emit(new BoxPath(root, rootInfo, info.asInstanceOf[BoxFile#Info].asLeft))
              case _ if BoxStore.isFolder(info) =>
                Stream
                  .fromBlockingIterator(
                    blocker,
                    BoxStore.blockingIterator(new BoxFolder(api, info.getID), fields)
                  )
                  .map {
                    case i if BoxStore.isFile(i)   => i.asInstanceOf[BoxFile#Info].asLeft.some
                    case i if BoxStore.isFolder(i) => i.asInstanceOf[BoxFolder#Info].asRight.some
                    case _                         => none
                  }
                  .unNone
                  .map(fileOrFolder => new BoxPath(root, rootInfo, fileOrFolder))
              case _ => Stream.empty
            }
        }
      case None => Stream.empty
    }
  }

  private def boxFileAtPath(path: Path): F[Option[BoxFile]] =
    boxInfoAtPath(path).map(_.flatMap {
      case info if BoxStore.isFile(info) =>
        Some(new BoxFile(api, info.getID))
      case _ => None
    })

  private def boxInfoAtPath(path: Path, fields: Array[String] = Array.empty): F[Option[BoxItem#Info]] =
    BoxPath.narrow(path).map(_.fileOrFolder.fold[BoxItem#Info](identity, identity)) match {
      case None =>
        boxInfoAtPathStream(rootFolder, BoxPath.rootFilePath(path), fields).compile.last
      case someInfo =>
        someInfo.pure
    }

  private def boxInfoAtPathStream(
    parentFolder: BoxFolder,
    pathParts: List[String],
    fields: Array[String]
  ): Stream[F, BoxItem#Info] =
    pathParts match {
      case Nil =>
        Stream.empty
      case head :: Nil =>
        Stream
          .fromBlockingIterator(blocker, BoxStore.blockingIterator(parentFolder, fields))
          .find(_.getName.equalsIgnoreCase(head))
      case head :: tail =>
        Stream
          .fromBlockingIterator(blocker, BoxStore.blockingIterator(parentFolder, Array.empty))
          .find(info => BoxStore.isFolder(info) && info.getName.equalsIgnoreCase(head))
          .flatMap(info => boxInfoAtPathStream(new BoxFolder(api, info.getID), tail, fields))
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
}

object BoxStore {
  def apply[F[_]](
    api: BoxAPIConnection,
    rootFolderId: String,
    blocker: Blocker
  )(implicit F: Concurrent[F], CS: ContextShift[F]): BoxStore[F] = new BoxStore(api, rootFolderId, blocker)

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

}
