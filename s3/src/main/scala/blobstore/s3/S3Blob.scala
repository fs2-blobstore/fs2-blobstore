package blobstore.s3

import java.time.Instant
import blobstore.url.{FsObject, FsObjectLowPri}
import blobstore.url.general.{GeneralStorageClass, StorageClassLookup}
import software.amazon.awssdk.services.s3.model.StorageClass

case class S3Blob(bucket: String, key: String, meta: Option[S3MetaInfo]) extends FsObject {
  override def name: String = key

  override type StorageClassType = StorageClass

  override def size: Option[Long] = meta.flatMap(_.size)

  override def isDir: Boolean = meta.fold(true)(_ => false)

  override def lastModified: Option[Instant] = meta.flatMap(_.lastModified)

  override private[blobstore] def generalStorageClass: Option[GeneralStorageClass] =
    meta.flatMap(_.storageClass).map {
      case StorageClass.GLACIER | StorageClass.DEEP_ARCHIVE => GeneralStorageClass.ColdStorage
      case _                                                => GeneralStorageClass.Standard
    }
}

object S3Blob extends FsObjectLowPri {
  implicit val storageClassLookup: StorageClassLookup.Aux[S3Blob, StorageClass] = new StorageClassLookup[S3Blob] {
    override type StorageClassType = StorageClass

    override def storageClass(a: S3Blob): Option[StorageClass] = a.meta.flatMap(_.storageClass)
  }
}
