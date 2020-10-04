package blobstore.s3

import java.time.Instant

import blobstore.url.FileSystemObject
import blobstore.url.general.GeneralStorageClass
import software.amazon.awssdk.services.s3.model.StorageClass

case class S3Blob(bucket: String, key: String, meta: Option[S3MetaInfo])

object S3Blob {
  implicit val fileSystemObject: FileSystemObject[S3Blob] = new FileSystemObject[S3Blob] {
    override def name(a: S3Blob): String = a.key

    override def size(a: S3Blob): Option[Long] = a.meta.flatMap(_.size)

    override def isDir(a: S3Blob): Boolean = a.meta.fold(true)(_ => false)

    override def lastModified(a: S3Blob): Option[Instant] = a.meta.flatMap(_.lastModified)

    override def storageClass(a: S3Blob): Option[GeneralStorageClass] = a.meta.flatMap(_.storageClass).map {
      case StorageClass.GLACIER | StorageClass.DEEP_ARCHIVE => GeneralStorageClass.ColdStorage
      case _                                                => GeneralStorageClass.Standard
    }
  }
}
