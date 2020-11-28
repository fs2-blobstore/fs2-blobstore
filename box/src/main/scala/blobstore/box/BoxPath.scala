package blobstore.box

import java.time.Instant

import blobstore.url.{FileSystemObject, Path}
import blobstore.url.general.UniversalFileSystemObject
import com.box.sdk.{BoxFile, BoxFolder, BoxItem}
import cats.syntax.option._

case class BoxPath(fileOrFolder: Either[BoxFile#Info, BoxFolder#Info]) {
  def file: Option[BoxFile#Info]     = fileOrFolder.swap.toOption
  def folder: Option[BoxFolder#Info] = fileOrFolder.toOption

  def lub: BoxItem#Info = fileOrFolder.fold(identity, identity)

}

object BoxPath {

  implicit val blob: FileSystemObject.Aux[BoxPath, Nothing] = new FileSystemObject[BoxPath] {
    type StorageClassType = Nothing

    override def name(a: BoxPath): String = a.fileOrFolder match {
      case Left(file)    => file.getName
      case Right(folder) => folder.getName
    }

    override def size(a: BoxPath): Option[Long] = a.fileOrFolder match {
      case Left(file)    => file.getSize.some
      case Right(folder) => folder.getSize.some
    }

    override def isDir(a: BoxPath): Boolean = a.fileOrFolder.isRight

    override def lastModified(a: BoxPath): Option[Instant] =
      a.fileOrFolder match {
        case Left(file)    => file.getModifiedAt.toInstant.some
        case Right(folder) => folder.getModifiedAt.toInstant.some
      }

    // Box doesn't have the concept of storage classes
    override def storageClass(a: BoxPath): Option[StorageClassType] = None

    override def universal(a: BoxPath): UniversalFileSystemObject =
      UniversalFileSystemObject(
        name(a),
        size(a),
        isDir(a),
        storageClass(a),
        lastModified(a)
      )
  }

  def narrow[A](p: Path[A]): Option[Path[BoxPath]] = p.representation match {
    case bp: BoxPath => p.as(bp: BoxPath).some
    case _           => None
  }
}
