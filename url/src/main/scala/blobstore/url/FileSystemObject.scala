package blobstore.url

import java.time.Instant

import blobstore.url.general.UniversalFileSystemObject

/** A FileSystemObject is an object in the underlying file system
  */
trait FileSystemObject[-A] {
  type StorageClassType

  def name(a: A): String

  def size(a: A): Option[Long]

  def isDir(a: A): Boolean

  def lastModified(a: A): Option[Instant]

  def storageClass(a: A): Option[StorageClassType]

  def universal(a: A): UniversalFileSystemObject
}

object FileSystemObject {
  def apply[A: FileSystemObject]: FileSystemObject[A] = implicitly[FileSystemObject[A]]

}
