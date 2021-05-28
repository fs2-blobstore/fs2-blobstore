package blobstore.gcs

import blobstore.url.{FsObject, FsObjectLowPri}
import blobstore.url.general.{GeneralStorageClass, StorageClassLookup}
import com.google.cloud.storage.{BlobInfo, StorageClass}

import java.time.Instant

// This type exists only to put the FileSystemObject instance on the default implicit search path
case class GcsBlob(blob: BlobInfo) extends FsObject {
  override type StorageClassType = StorageClass

  override def name: String = blob.getName

  override def size: Option[Long] = Option(blob.getSize.toLong)

  override def isDir: Boolean = blob.isDirectory

  override def lastModified: Option[Instant] = Option(blob.getUpdateTime).map(Instant.ofEpochMilli(_))

  override private[blobstore] def generalStorageClass: Option[GeneralStorageClass] =
    Option(blob.getStorageClass).map {
      case StorageClass.COLDLINE | StorageClass.ARCHIVE => GeneralStorageClass.ColdStorage
      case _                                            => GeneralStorageClass.Standard
    }
}

object GcsBlob extends FsObjectLowPri {
  implicit val storageClassLookup: StorageClassLookup.Aux[GcsBlob, StorageClass] = new StorageClassLookup[GcsBlob] {
    override type StorageClassType = StorageClass

    override def storageClass(fso: GcsBlob): Option[StorageClass] = Option(fso.blob.getStorageClass)

  }
}
