package blobstore.box

import java.time.Instant
import blobstore.url.{FsObject, FsObjectLowPri, Path}
import blobstore.url.general.{GeneralStorageClass, StorageClassLookup}
import cats.syntax.option.*
import com.box.sdkgen.schemas.filefull.FileFull
import com.box.sdkgen.schemas.folderfull.FolderFull

case class BoxPath(fileOrFolder: Either[FileFull, FolderFull]) extends FsObject {
  def file: Option[FileFull]     = fileOrFolder.swap.toOption
  def folder: Option[FolderFull] = fileOrFolder.toOption

  override type StorageClassType = Nothing

  override def name: String = fileOrFolder match {
    case Left(file)    => file.getName
    case Right(folder) => folder.getName
  }

  override def size: Option[Long] = fileOrFolder match {
    case Left(file)    => Option(file.getSize).map(_.longValue)
    case Right(folder) => Option(folder.getSize).map(_.longValue)
  }

  override def isDir: Boolean = fileOrFolder.isRight

  override def lastModified: Option[Instant] = fileOrFolder match {
    case Left(file)    => Option(file.getModifiedAt).map(_.toInstant)
    case Right(folder) => Option(folder.getModifiedAt).map(_.toInstant)
  }

  override def created: Option[Instant] = fileOrFolder match {
    case Left(file)    => Option(file.getCreatedAt).map(_.toInstant)
    case Right(folder) => Option(folder.getCreatedAt).map(_.toInstant)
  }

  override private[blobstore] def generalStorageClass: Option[GeneralStorageClass] = None

}

object BoxPath extends FsObjectLowPri {
  // scalafix:off
  def narrow[A](p: Path[A]): Option[Path[BoxPath]] =
    if (p.representation.isInstanceOf[BoxPath]) {
      p.as(p.representation.asInstanceOf[BoxPath]).some
    } else None
  // scalafix:on

  implicit val storageClassLookup: StorageClassLookup.Aux[BoxPath, Nothing] = new StorageClassLookup[BoxPath] {
    override type StorageClassType = Nothing

    override def storageClass(a: BoxPath): None.type = None
  }
}
