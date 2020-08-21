package blobstore.url.general

import java.time.Instant

import blobstore.url.FileSystemObject

/**
  * This type represents a product of properties that are typically present in file system objects
  *
  * It's used to represent underlying file system objects in stores that abstracts multiple file systems
  */
case class UniversalFileSystemObject(
  name: String,
  size: Option[Long],
  isDir: Boolean,
  storageClass: Option[GeneralStorageClass],
  lastModified: Option[Instant]
)

object UniversalFileSystemObject {
  implicit val fileSystemObject: FileSystemObject[UniversalFileSystemObject] =
    new FileSystemObject[UniversalFileSystemObject] {
      type StorageClassType = GeneralStorageClass

      override def name(a: UniversalFileSystemObject): String = a.name

      override def size(a: UniversalFileSystemObject): Option[Long] = a.size

      override def isDir(a: UniversalFileSystemObject): Boolean = a.isDir

      override def lastModified(a: UniversalFileSystemObject): Option[Instant] = a.lastModified

      override def storageClass(a: UniversalFileSystemObject): Option[StorageClassType] = a.storageClass

      override def universal(a: UniversalFileSystemObject): UniversalFileSystemObject = a
    }
}
