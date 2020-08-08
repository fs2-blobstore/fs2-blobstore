package blobstore.experiment.fs

import java.nio.file.{Files, Path}
import java.time.Instant

import blobstore.experiment.url.Blob


case class NioPath(value: Path)

object NioPath {
  implicit val blob: Blob[NioPath] = new Blob[NioPath] {
    override def size(a: NioPath): Long = Files.size(a.value)

    override def isDir(a: NioPath): Boolean = Files.isDirectory(a.value)

    override def lastModified(a: NioPath): Option[Instant] = Option(Files.getLastModifiedTime(a.value)).map(_.toInstant)
  }
}
