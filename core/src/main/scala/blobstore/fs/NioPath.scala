package blobstore.fs

import java.nio.file.{Files, Path => JPath}
import java.time.Instant

import blobstore.url.FileSystemObject
import blobstore.url.general.{GeneralStorageClass, UniversalFileSystemObject}

// Cache lookups done on read
case class NioPath(path: JPath, size: Option[Long], isDir: Boolean, lastModified: Option[Instant])

object NioPath {

  implicit val filesystemObject: FileSystemObject[NioPath] = new FileSystemObject[NioPath] {
    override def name(a: NioPath): String = a.path.toString.toString

    override def size(a: NioPath): Option[Long] = a.size

    override def isDir(a: NioPath): Boolean = a.isDir

    override def lastModified(a: NioPath): Option[Instant] = a.lastModified

    override def storageClass(a: NioPath): Option[GeneralStorageClass] = None
  }
}
