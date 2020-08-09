package blobstore.url

import java.time.Instant

trait Blob[-A] {
  def size(a: A): Long

  def isDir(a: A): Boolean

  def lastModified(a: A): Option[Instant]
}

object Blob {
  def apply[A: Blob]: Blob[A] = implicitly[Blob[A]]

}