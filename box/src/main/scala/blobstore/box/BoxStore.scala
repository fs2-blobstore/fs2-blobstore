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
import com.box.sdkgen.client.BoxClient
import com.box.sdkgen.managers.files.{
  CopyFileRequestBody,
  CopyFileRequestBodyParentField,
  UpdateFileByIdRequestBody,
  UpdateFileByIdRequestBodyParentField
}
import com.box.sdkgen.managers.folders.{
  CreateFolderRequestBody,
  CreateFolderRequestBodyParentField,
  DeleteFolderByIdQueryParams,
  GetFolderItemsQueryParams
}
import com.box.sdkgen.managers.uploads.{
  UploadFileRequestBody,
  UploadFileRequestBodyAttributesField,
  UploadFileRequestBodyAttributesParentField,
  UploadFileVersionRequestBody,
  UploadFileVersionRequestBodyAttributesField
}
import com.box.sdkgen.schemas.filefull.FileFull
import com.box.sdkgen.schemas.folderfull.FolderFull
import com.box.sdkgen.schemas.item.Item
import fs2.{Pipe, Stream}

import java.io.{InputStream, OutputStream, PipedInputStream, PipedOutputStream}
import scala.jdk.CollectionConverters.*

/** @param client
  *   underlying configured BoxClient
  * @param rootFolderId
  *   Root Folder Id, default – "0"
  * @param largeFileThreshold
  *   override for the threshold on the file size to be considered "large", default – 50MiB
  */
class BoxStore[F[_]: Async](
  client: BoxClient,
  rootFolderId: String,
  largeFileThreshold: Long
) extends PathStore[F, BoxPath] {

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[BoxPath]] =
    listUnderlying(path, List.empty, recursive)

  override def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte] =
    Stream.eval(boxFileIdAtPath(path)).flatMap {
      case None =>
        Stream.raiseError[F](new IllegalArgumentException(show"Item at path '$path' doesn't exist or is not a File"))
      case Some(fileId) =>
        Stream.eval(Async[F].blocking(client.downloads.downloadFile(fileId))).flatMap { is =>
          fs2.io.readInputStream(is.pure[F], chunkSize, closeAfterUse = true)
        }
    }

  override def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = {
    in =>
      path.lastSegment match {
        case None =>
          Stream.raiseError[F](new IllegalArgumentException(show"Specified path '$path' doesn't point to a file."))
        case Some(name) =>
          // Either[fileId: String, folderId: String] – Left = existing file to overwrite, Right = parent folder for new file
          val init: F[(OutputStream, InputStream, Either[String, String])] = {
            val os = new PipedOutputStream()
            val is = new PipedInputStream(os)
            boxFileIdAtPath(path).flatMap {
              case Some(existingId) if overwrite =>
                (os: OutputStream, is: InputStream, existingId.asLeft[String]).pure[F]
              case Some(_) =>
                Async[F].raiseError(new IllegalArgumentException(show"File at path '$path' already exist."))
              case None =>
                putFolderAtPath(rootFolderId, path.up.segments.toList).map { parentFolderId =>
                  (os: OutputStream, is: InputStream, parentFolderId.asRight[String])
                }
            }
          }

          val consume: ((OutputStream, InputStream, Either[String, String])) => Stream[F, Unit] = ios => {
            val putToBox = Stream.eval(Async[F].blocking {
              size match {
                case Some(s) if s > largeFileThreshold =>
                  ios._3 match {
                    case Right(folderId) =>
                      client.chunkedUploads.uploadBigFile(ios._2, name, s, folderId)
                    case Left(existingFileId) =>
                      // Large file version update: delete old + upload new via chunked upload
                      val parentFolderId = client.files.getFileById(existingFileId).getParent.getId
                      client.files.deleteFileById(existingFileId)
                      client.chunkedUploads.uploadBigFile(ios._2, name, s, parentFolderId)
                  }
                case _ =>
                  ios._3 match {
                    case Left(existingFileId) =>
                      val attrs = new UploadFileVersionRequestBodyAttributesField(name)
                      client.uploads.uploadFileVersion(existingFileId, new UploadFileVersionRequestBody(attrs, ios._2))
                    case Right(folderId) =>
                      val parent = new UploadFileRequestBodyAttributesParentField(folderId)
                      val attrs  = new UploadFileRequestBodyAttributesField(name, parent)
                      client.uploads.uploadFile(new UploadFileRequestBody(attrs, ios._2))
                  }
              }
            }.void)

            val writeBytes = {
              in.through(fs2.io.writeOutputStream(ios._1.pure, closeAfterUse = false)) ++
                Stream.eval(Async[F].delay(ios._1.close()))
            }
            putToBox.concurrently(writeBytes)
          }

          val release: ((OutputStream, InputStream, Either[String, String])) => F[Unit] = ios =>
            Async[F].blocking {
              ios._2.close()
              ios._1.close()
            }

          Stream.bracket(init)(release).flatMap(consume)
      }
  }

  override def move[A, B](src: Path[A], dst: Path[B]): F[Unit] = boxFileIdAtPath(src).flatMap {
    case Some(fileId) =>
      putFolderAtPath(rootFolderId, dst.up.segments.toList)
        .flatMap { folderId =>
          val dstName = dst.lastSegment.filter(s => !s.endsWith("/")).getOrElse {
            client.files.getFileById(fileId).getName
          }
          Async[F].blocking {
            val parent = new UpdateFileByIdRequestBodyParentField.Builder().id(folderId).build()
            val body   = new UpdateFileByIdRequestBody.Builder().name(dstName).parent(parent).build()
            client.files.updateFileById(fileId, body)
          }.void
        }
    case None => ().pure[F]
  }

  override def copy[A, B](src: Path[A], dst: Path[B]): F[Unit] = boxFileIdAtPath(src).flatMap {
    case Some(fileId) =>
      putFolderAtPath(rootFolderId, dst.up.segments.toList)
        .flatMap { folderId =>
          val dstName = dst.lastSegment.filter(s => !s.endsWith("/")).getOrElse {
            client.files.getFileById(fileId).getName
          }
          Async[F].blocking {
            val parent = new CopyFileRequestBodyParentField(folderId)
            val body   = new CopyFileRequestBody.Builder(parent).name(dstName).build()
            client.files.copyFile(fileId, body)
          }.void
        }
    case None => ().pure[F]
  }

  override def remove[A](path: Path[A], recursive: Boolean): F[Unit] =
    boxInfoAtPath(path).flatMap {
      case Some(p) => p.representation.fileOrFolder.fold(
          file => Async[F].blocking(client.files.deleteFileById(file.getId)),
          folder =>
            Async[F].blocking {
              val params = new DeleteFolderByIdQueryParams.Builder().recursive(recursive).build()
              client.folders.deleteFolderById(folder.getId, params)
            }
        )
      case None => ().pure
    }

  override def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, OutputStream] = for {
      p    <- Resource.eval(computePath)
      name <- Resource.eval(
        Async[F].fromEither(p.lastSegment.filter(s => !s.endsWith("/")).toRight(new IllegalArgumentException(
          show"Specified path '$p' doesn't point to a file."
        )))
      )
      fileOrFolder <- Resource.eval(boxFileIdAtPath(p).flatMap {
        case None         => putFolderAtPath(rootFolderId, p.up.segments.toList).map(_.asRight[String])
        case Some(fileId) => fileId.asLeft[String].pure[F]
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
            fileOrFolder match {
              case Left(existingFileId) =>
                val attrs = new UploadFileVersionRequestBodyAttributesField(name)
                client.uploads.uploadFileVersion(existingFileId, new UploadFileVersionRequestBody(attrs, is))
              case Right(folderId) =>
                val parent = new UploadFileRequestBodyAttributesParentField(folderId)
                val attrs  = new UploadFileRequestBodyAttributesField(name, parent)
                client.uploads.uploadFile(new UploadFileRequestBody(attrs, is))
            }
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
    fields: List[String] = List.empty,
    recursive: Boolean
  ): Stream[F, Path[BoxPath]] = {
    val stream: Stream[F, Path[BoxPath]] = Stream.eval(boxInfoAtPath(path, fields)).flatMap {
      case Some(info) =>
        info.representation.fileOrFolder match {
          case Left(_) =>
            Stream.emit(info)
          case Right(folder) =>
            listFolderItems(folder.getId, fields)
              .map { item =>
                if (item.isFileFull) item.getFileFull.asLeft[FolderFull].some
                else if (item.isFolderFull) item.getFolderFull.asRight[FileFull].some
                else none
              }
              .unNone
              .map { fileOrFolder =>
                val itemName = fileOrFolder.fold(_.getName, _.getName)
                (path / itemName).as(BoxPath(fileOrFolder))
              }
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

  private def boxFileIdAtPath[A](path: Path[A]): F[Option[String]] =
    boxInfoAtPath(path).map(_.flatMap(_.representation.file).map(_.getId))

  private def boxInfoAtPath[A](path: Path[A], fields: List[String] = List.empty): F[Option[Path[BoxPath]]] =
    BoxPath.narrow(path) match {
      case None =>
        boxInfoAtPathStream(rootFolderId, path.stripSlashSuffix.segments.toList, fields).compile.last.map {
          case Some(item) =>
            if (item.isFileFull) path.as(BoxPath(item.getFileFull.asLeft[FolderFull])).some
            else if (item.isFolderFull) path.as(BoxPath(item.getFolderFull.asRight[FileFull])).some
            else none[Path[BoxPath]]
          case None => None
        }
      case someInfo => someInfo.pure
    }

  private def boxInfoAtPathStream(
    parentFolderId: String,
    pathParts: List[String],
    fields: List[String]
  ): Stream[F, Item] =
    pathParts match {
      case Nil =>
        if (parentFolderId == rootFolderId)
          Stream.eval(Async[F].blocking(new Item(client.folders.getFolderById(parentFolderId))))
        else Stream.empty
      case head :: Nil =>
        listFolderItems(parentFolderId, fields)
          .find(_.getName.equalsIgnoreCase(head))
      case head :: tail =>
        listFolderItems(parentFolderId, List.empty)
          .find { item =>
            item.isFolderFull && item.getName.equalsIgnoreCase(head)
          }
          .flatMap { item =>
            boxInfoAtPathStream(item.getFolderFull.getId, tail, fields)
          }
    }

  /** Creates all folders along the given path, returning the ID of the leaf folder. */
  private def putFolderAtPath(parentFolderId: String, parts: List[String]): F[String] = parts match {
    case Nil          => parentFolderId.pure[F]
    case head :: tail =>
      listFolderItems(parentFolderId, List.empty)
        .find(_.getName.equalsIgnoreCase(head))
        .compile
        .last
        .flatMap[String] {
          case Some(item) if item.isFolderFull =>
            item.getFolderFull.getId.pure[F]
          case Some(_) =>
            Async[F].raiseError(
              new IllegalArgumentException(
                show"Can't create Folder '$head' along path - File with this name already exists"
              )
            )
          case None =>
            Async[F].blocking {
              val parent = new CreateFolderRequestBodyParentField(parentFolderId)
              val body   = new CreateFolderRequestBody(head, parent)
              client.folders.createFolder(body).getId
            }
        }
        .flatMap(putFolderAtPath(_, tail))
  }

  /** Paginates through folder items, emitting each Item. */
  private def listFolderItems(folderId: String, fields: List[String]): Stream[F, Item] = {
    val limit         = 1000L
    val requestFields = (BoxStore.defaultItemFields ++ fields).distinct
    Stream.unfoldEval(0L) { offset =>
      Async[F].blocking {
        val params = new GetFolderItemsQueryParams.Builder()
          .offset(offset)
          .limit(limit)
          .fields(requestFields.asJava)
          .build()
        val items   = client.folders.getFolderItems(folderId, params)
        val entries = items.getEntries.asScala.toList
        if (entries.isEmpty) None
        else Some((entries, offset + entries.size))
      }
    }.flatMap(Stream.emits)
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

  private val defaultItemFields = List("type", "id", "name", "size", "modified_at")

  def builder[F[_]: Async](boxClient: BoxClient): BoxStoreBuilder[F] =
    BoxStoreBuilderImpl[F](boxClient)

  /** @see
    *   [[BoxStore]]
    */
  trait BoxStoreBuilder[F[_]] {
    def withBoxClient(boxClient: BoxClient): BoxStoreBuilder[F]
    def withRootFolderId(rootFolderId: String): BoxStoreBuilder[F]
    def withLargeFileThreshold(largeFileThreshold: Long): BoxStoreBuilder[F]
    def build: ValidatedNec[Throwable, BoxStore[F]]
    def unsafe: BoxStore[F] = build match {
      case Validated.Valid(a)    => a
      case Validated.Invalid(es) => throw es.reduce(Throwables.collapsingSemigroup) // scalafix:ok
    }
  }

  case class BoxStoreBuilderImpl[F[_]: Async](
    _boxClient: BoxClient,
    _rootFolderId: String = "0",
    _largeFileThreshold: Long = 50L * 1024L * 1024L
  ) extends BoxStoreBuilder[F] {

    def withBoxClient(boxClient: BoxClient): BoxStoreBuilder[F] =
      this.copy(_boxClient = boxClient)

    def withRootFolderId(rootFolderId: String): BoxStoreBuilder[F] = this.copy(_rootFolderId = rootFolderId)

    def withLargeFileThreshold(largeFileThreshold: Long): BoxStoreBuilder[F] =
      this.copy(_largeFileThreshold = largeFileThreshold)

    def build: ValidatedNec[Throwable, BoxStore[F]] = {
      val validateLFT =
        if (_largeFileThreshold < 1024L * 1024L) {
          new IllegalArgumentException("Please consider increasing largeFileThreshold to at least 1MB.").invalidNec
        } else ().validNec
      validateLFT.map(_ => new BoxStore(_boxClient, _rootFolderId, _largeFileThreshold))
    }
  }
}
