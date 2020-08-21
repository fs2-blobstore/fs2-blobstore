package blobstore.url

import java.time.Instant

/**
  * A FileSystemObject is an object in the underlying file system
  */
trait FileSystemObject[-A] {
  def name(a: A): String

  def size(a: A): Long // TODO: Make Optin[Long]

  def isDir(a: A): Boolean

  def lastModified(a: A): Option[Instant]
}

object FileSystemObject {
  def apply[A: FileSystemObject]: FileSystemObject[A] = implicitly[FileSystemObject[A]]

}