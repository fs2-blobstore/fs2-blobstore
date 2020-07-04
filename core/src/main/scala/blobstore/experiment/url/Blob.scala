package blobstore.experiment.url

import java.time.Instant

trait Blob[-A] {
  def toUrl(a: A): Url.PlainUrl

  def authority(a: A): Authority

  def size(a: A): Long

  def isDir(a: A): Boolean

  def lastModified(a: A): Option[Instant]
}

object Blob {
  def apply[A: Blob]: Blob[A] = implicitly[Blob[A]]

}