package blobstore.box

import java.time.Instant
import blobstore.url.{FsObject, FsObjectLowPri, Path}
import blobstore.url.general.{GeneralStorageClass, StorageClassLookup}
import cats.syntax.option._
import com.box.sdk.{BoxFile, BoxFolder, BoxItem}

case class BoxPath(fileOrFolder: Either[BoxFile#Info, BoxFolder#Info]) extends FsObject {
  def file: Option[BoxFile#Info]     = fileOrFolder.swap.toOption
  def folder: Option[BoxFolder#Info] = fileOrFolder.toOption

  def lub: BoxItem#Info = fileOrFolder.fold[BoxItem#Info](identity, identity)

  override type StorageClassType = Nothing

  override def name: String = fileOrFolder match {
    case Left(file)    => file.getName
    case Right(folder) => folder.getName
  }

  override def size: Option[Long] = fileOrFolder match {
    case Left(file)    => file.getSize.some
    case Right(folder) => folder.getSize.some
  }

  override def isDir: Boolean = fileOrFolder.isRight

  override def lastModified: Option[Instant] = fileOrFolder match {
    case Left(file)    => file.getModifiedAt.toInstant.some
    case Right(folder) => folder.getModifiedAt.toInstant.some
  }

  override private[blobstore] def generalStorageClass: Option[GeneralStorageClass] = None

}

object BoxPath extends FsObjectLowPri {
  def narrow[A](p: Path[A]): Option[Path[BoxPath]] = p.representation match {
    case bp: BoxPath => p.as(bp: BoxPath).some
    case _           => None
  }

  implicit val storageClassLookup: StorageClassLookup.Aux[BoxPath, Nothing] = new StorageClassLookup[BoxPath] {
    override type StorageClassType = Nothing

    override def storageClass(a: BoxPath): None.type = None
  }
}
