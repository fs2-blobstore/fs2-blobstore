package blobstore.url.general

import java.time.Instant

import blobstore.url.FileSystemObject

/**
 * This type represents a product of properties that are typically present in file system objects
 *
 * It's used to represent underlying file system objects in stores that abstracts multiple file systems
 */
case class GeneralFileSystemObject(
  name: String,
  size: Option[Long],
  isDir: Boolean,
  storageClass: Option[GeneralStorageClass],
  lastModified: Option[Instant]
)

object GeneralFileSystemObject {
  implicit val fileSystemObject: FileSystemObject[GeneralFileSystemObject] = new FileSystemObject[GeneralFileSystemObject] {
    override def name(a: GeneralFileSystemObject): String = a.name

    override def size(a: GeneralFileSystemObject): Option[Long] = a.size

    override def isDir(a: GeneralFileSystemObject): Boolean = a.isDir

    override def lastModified(a: GeneralFileSystemObject): Option[Instant] = a.lastModified
  }
}