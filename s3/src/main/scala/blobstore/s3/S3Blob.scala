package blobstore.s3

import java.time.Instant

import blobstore.url.FsObject
import blobstore.url.general.GeneralStorageClass
import software.amazon.awssdk.services.s3.model.StorageClass

case class S3Blob(bucket: String, key: String, meta: Option[S3MetaInfo]) extends FsObject {
  override def name: String = key

  override type StorageClassType = StorageClass

  override def size: Option[Long] = meta.flatMap(_.size)

  override def isDir: Boolean = meta.fold(true)(_ => false)

  override def lastModified: Option[Instant] = meta.flatMap(_.lastModified)

  override def storageClass: Option[StorageClass] = meta.flatMap(_.storageClass)

  override def generalStorageClass: Option[GeneralStorageClass] =
    storageClass.map {
      case StorageClass.GLACIER | StorageClass.DEEP_ARCHIVE => GeneralStorageClass.ColdStorage
      case _                                                => GeneralStorageClass.Standard
    }
}
