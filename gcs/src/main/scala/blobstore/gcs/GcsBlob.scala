package blobstore.gcs

import java.time.Instant

import blobstore.url.FileSystemObject
import blobstore.url.general.{GeneralStorageClass, UniversalFileSystemObject}
import com.google.cloud.storage.{BlobInfo, StorageClass}

// This can be a newtype, we only need it to add an instance to the default implicit search path
case class GcsBlob(blob: BlobInfo) extends AnyVal
object GcsBlob {
  def toUniversal(blob: GcsBlob): UniversalFileSystemObject = {
    val storageClass = blob.blob.getStorageClass match {
      case StorageClass.COLDLINE | StorageClass.ARCHIVE => GeneralStorageClass.ColdStorage
      case _                                            => GeneralStorageClass.Standard
    }

    val fso = FileSystemObject[GcsBlob]
    UniversalFileSystemObject(
      name = fso.name(blob),
      size = fso.size(blob),
      isDir = fso.isDir(blob),
      storageClass = Option(storageClass),
      lastModified = fso.lastModified(blob)
    )
  }
  
  
  implicit val fileSystemObject: FileSystemObject[GcsBlob] = new FileSystemObject[GcsBlob] {
    override def name(a: GcsBlob): String = a.blob.getName

    override def size(a: GcsBlob): Option[Long] = Option(a.blob.getSize.toLong)

    override def isDir(a: GcsBlob): Boolean = a.blob.isDirectory

    override def lastModified(a: GcsBlob): Option[Instant] = Option(a.blob.getUpdateTime).map(Instant.ofEpochMilli(_))
  }
}
