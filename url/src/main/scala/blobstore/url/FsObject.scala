package blobstore.url

import java.time.Instant

import blobstore.url.general.GeneralStorageClass

trait FsObject {
  type StorageClassType

  def name: String

  def size: Option[Long]

  def isDir: Boolean

  def lastModified: Option[Instant]

  def storageClass: Option[StorageClassType]

  def generalStorageClass: Option[GeneralStorageClass]
}

object FsObject {
  type Aux[R] = FsObject { type StorageClassType = R }
}
