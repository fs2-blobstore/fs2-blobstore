package blobstore.url

import blobstore.url.general.{GeneralStorageClass, StorageClassLookup}

import java.time.Instant

trait FsObject {
  type StorageClassType

  def name: String

  def size: Option[Long]

  def isDir: Boolean

  def lastModified: Option[Instant]

  def storageClass(implicit scl: StorageClassLookup[this.type]): Option[scl.StorageClassType] =
    scl.storageClass(this)

  private[blobstore] def generalStorageClass: Option[GeneralStorageClass]
}

object FsObject extends FsObjectLowPri {
  type Aux[R] = FsObject { type StorageClassType = R }
}

trait FsObjectLowPri {
  implicit val dependent: StorageClassLookup.Aux[FsObject, GeneralStorageClass] = new StorageClassLookup[FsObject] {
    override type StorageClassType = GeneralStorageClass

    override def storageClass(fso: FsObject): Option[GeneralStorageClass] = fso.generalStorageClass
  }
}
