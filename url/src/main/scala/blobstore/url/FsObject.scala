package blobstore.url

import java.time.Instant

/**
  * A FsObject, or FileSystemObject, is an object in the underlying file system
  *
  * For flat filesystems like GCS, S3 and Azure, there's a one-to-one correspondence with blobs and FsObject
  * For hierarchical filesystems, like SFTP or the local file system, a file system object may be directories or files.
  */
trait FsObject[-A] {
  def name(a: A): String

  def size(a: A): Long

  def isDir(a: A): Boolean

  def lastModified(a: A): Option[Instant]
}

object FsObject {
  def apply[A: FsObject]: FsObject[A] = implicitly[FsObject[A]]

}