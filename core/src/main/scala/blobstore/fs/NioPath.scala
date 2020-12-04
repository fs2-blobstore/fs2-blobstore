package blobstore.fs

import java.nio.file.{Path => JPath}
import java.time.Instant

import blobstore.url.FsObject
import blobstore.url.general.GeneralStorageClass

// Cache lookups done on read
case class NioPath(path: JPath, size: Option[Long], isDir: Boolean, lastModified: Option[Instant]) extends FsObject {
  override type StorageClassType = Nothing

  override def name: String = path.toString

  override def storageClass: Option[Nothing] = None

  override def generalStorageClass: Option[GeneralStorageClass] = None
}
