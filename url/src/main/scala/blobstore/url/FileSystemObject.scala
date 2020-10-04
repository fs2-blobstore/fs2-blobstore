package blobstore.url

import java.time.Instant

import blobstore.url.general.{GeneralStorageClass, UniversalFileSystemObject}

/**
  * A FileSystemObject is an object in the underlying file system
  */
trait FileSystemObject[-A] {
  def name(a: A): String

  def size(a: A): Option[Long]

  def isDir(a: A): Boolean

  def lastModified(a: A): Option[Instant]

  def storageClass(a: A): Option[GeneralStorageClass]

  def universal(a: A): UniversalFileSystemObject = UniversalFileSystemObject(
    name(a),
    size(a),
    isDir(a),
    storageClass(a),
    lastModified(a)
  )
}

object FileSystemObject {
  def apply[A: FileSystemObject]: FileSystemObject[A] = implicitly[FileSystemObject[A]]

}
