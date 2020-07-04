package blobstore.experiment.fs

import java.nio.file.{Files, Path, Paths}
import java.time.Instant

import blobstore.experiment.url.{Authority, Blob}
import blobstore.experiment.url.Authority.StandardAuthority
import blobstore.experiment.url.Url.PlainUrl


case class NioPath(value: Path)

object NioPath {
  implicit val blob: Blob[NioPath] = new Blob[NioPath] {
    /**
      * TODO: Revisit this
      *
      * Relevant staandards
      *
      * file URI https://tools.ietf.org/html/rfc8089#section-2
      * URL standard section on file URL https://tools.ietf.org/html/rfc1738
      */
    override def toUrl(a: NioPath): PlainUrl = PlainUrl.unsafe(a.value.toUri.toString)

    override def authority(a: NioPath): Authority = StandardAuthority.parse("localhost")

    override def size(a: NioPath): Long = Files.size(a.value)

    override def isDir(a: NioPath): Boolean = Files.isDirectory(a.value)

    override def lastModified(a: NioPath): Option[Instant] = Option(Files.getLastModifiedTime(a.value)).toInstant
  }
}
