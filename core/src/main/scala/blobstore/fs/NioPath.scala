package blobstore.fs

import java.nio.file.{Path => JPath}
import java.time.Instant

import blobstore.url.FileSystemObject
import blobstore.url.general.UniversalFileSystemObject

// Cache lookups done on read
case class NioPath(path: JPath, size: Option[Long], isDir: Boolean, lastModified: Option[Instant])

object NioPath {

  implicit val filesystemObject: FileSystemObject[NioPath] = new FileSystemObject[NioPath] {
    type StorageClassType = Nothing

    override def name(a: NioPath): String = a.path.toString.toString

    override def size(a: NioPath): Option[Long] = a.size

    override def isDir(a: NioPath): Boolean = a.isDir

    override def lastModified(a: NioPath): Option[Instant] = a.lastModified

    override def storageClass(a: NioPath): Option[StorageClassType] = None

    override def universal(a: NioPath): UniversalFileSystemObject =
      UniversalFileSystemObject(
        name(a),
        size(a),
        isDir(a),
        storageClass(a),
        lastModified(a)
      )
  }
}
