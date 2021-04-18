package blobstore.fs

import java.nio.file.{Path => JPath}
import java.time.Instant
import blobstore.url.{FsObject, FsObjectLowPri}
import blobstore.url.general.{GeneralStorageClass, StorageClassLookup}

// Cache lookups done on read
case class NioPath(path: JPath, size: Option[Long], isDir: Boolean, lastModified: Option[Instant]) extends FsObject {
  override type StorageClassType = Nothing

  override def name: String = path.toString

  override private[blobstore] def generalStorageClass: Option[GeneralStorageClass] = None
}

object NioPath extends FsObjectLowPri {

  implicit val storageClassLookup: StorageClassLookup.Aux[NioPath, Nothing] = new StorageClassLookup[NioPath] {
    override type StorageClassType = Nothing

    override def storageClass(a: NioPath): None.type = None
  }
}
