package blobstore.gcs

import java.time.Instant

import blobstore.url.FileSystemObject
import blobstore.url.general.{GeneralFileSystemObject, GeneralStorageClass}
import com.google.cloud.storage.{Blob, StorageClass}

// This can be a newtype, we only need it to add an instance to the default implicit search path
case class GcsBlob(blob: Blob)
object GcsBlob {
  def toGeneral(blob: GcsBlob): GeneralFileSystemObject = {
    val storageClass = blob.blob.getStorageClass match {
      case StorageClass.COLDLINE | StorageClass.ARCHIVE => GeneralStorageClass.ColdStorage
      case _                                            => GeneralStorageClass.Standard
    }

    val fso = FileSystemObject[GcsBlob]
    GeneralFileSystemObject(
      name = fso.name(blob),
      size = fso.size(blob),
      isDir = fso.isDir(blob),
      storageClass = Option(storageClass),
      lastModified = fso.lastModified(blob),
      created = fso.created(blob)
    )
  }
  
  
  implicit val fileSystemObject: FileSystemObject[GcsBlob] = new FileSystemObject[GcsBlob] {
    override def name(a: GcsBlob): String = a.blob.getName

    override def size(a: GcsBlob): Option[Long] = Option(a.blob.getSize.toLong)

    override def isDir(a: GcsBlob): Boolean = a.blob.isDirectory

    override def lastModified(a: GcsBlob): Option[Instant] = Option(a.blob.getUpdateTime).map(Instant.ofEpochMilli(_))

    override def created(a: GcsBlob): Option[Instant] = Option(a.blob.getCreateTime).map(Instant.ofEpochMilli(_))
  }
}
