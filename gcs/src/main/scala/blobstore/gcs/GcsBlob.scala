package blobstore.gcs

import java.time.Instant

import blobstore.url.FileSystemObject
import blobstore.url.general.{GeneralStorageClass, UniversalFileSystemObject}
import com.google.cloud.storage.{BlobInfo, StorageClass}

// This type exists only to put the FileSystemObject instance on the default implicit search path
case class GcsBlob(blob: BlobInfo) extends AnyVal
object GcsBlob {

  implicit val fileSystemObject: FileSystemObject[GcsBlob] = new FileSystemObject[GcsBlob] {
    override def name(a: GcsBlob): String = a.blob.getName

    override def size(a: GcsBlob): Option[Long] = Option(a.blob.getSize.toLong)

    override def isDir(a: GcsBlob): Boolean = a.blob.isDirectory

    override def lastModified(a: GcsBlob): Option[Instant] = Option(a.blob.getUpdateTime).map(Instant.ofEpochMilli(_))

    override def storageClass(a: GcsBlob): Option[GeneralStorageClass] = a.blob.getStorageClass match {
      case StorageClass.COLDLINE | StorageClass.ARCHIVE => Some(GeneralStorageClass.ColdStorage)
      case _                                            => Some(GeneralStorageClass.Standard)
    }
  }
}
