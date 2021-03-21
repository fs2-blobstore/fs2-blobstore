package blobstore.gcs

import blobstore.url.FsObject
import blobstore.url.general.GeneralStorageClass
import com.google.cloud.storage.{BlobInfo, StorageClass}

import java.time.Instant

// This type exists only to put the FileSystemObject instance on the default implicit search path
case class GcsBlob(blob: BlobInfo) extends FsObject {
  override def name: String = blob.getName

  override type StorageClassType = StorageClass

  override def size: Option[Long] = Option(blob.getSize.toLong)

  override def isDir: Boolean = blob.isDirectory

  override def lastModified: Option[Instant] = Option(blob.getUpdateTime).map(Instant.ofEpochMilli(_))

  override def storageClass: Option[StorageClass] = Option(blob.getStorageClass)

  override def generalStorageClass: Option[GeneralStorageClass] =
    storageClass.map {
      case StorageClass.COLDLINE | StorageClass.ARCHIVE => GeneralStorageClass.ColdStorage
      case _                                            => GeneralStorageClass.Standard
    }
}
