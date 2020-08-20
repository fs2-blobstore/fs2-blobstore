package blobstore.experiment.fs

import java.nio.file.{Files, Path}
import java.time.Instant

import blobstore.url.FsObject


case class NioPath(value: Path)

object NioPath {
  implicit val blob: FsObject[NioPath] = new FsObject[NioPath] {
    override def name(a: NioPath): String = a.value.getFileName.toString

    override def size(a: NioPath): Long = Files.size(a.value)

    override def isDir(a: NioPath): Boolean = Files.isDirectory(a.value)

    override def lastModified(a: NioPath): Option[Instant] = Option(Files.getLastModifiedTime(a.value)).map(_.toInstant)

  }
}
