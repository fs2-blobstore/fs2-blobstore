package blobstore.sftp.experiment

import java.time.Instant

import blobstore.url.FsObject
import com.jcraft.jsch.SftpATTRS

case class SftpFsElement(name: String, attrs: SftpATTRS)

object SftpFsElement {
  implicit val blob: FsObject[SftpFsElement] = new FsObject[SftpFsElement] {
    override def name(a: SftpFsElement): String = a.name

    override def size(a: SftpFsElement): Long = a.attrs.getSize

    override def isDir(a: SftpFsElement): Boolean = a.attrs.isDir

    override def lastModified(a: SftpFsElement): Option[Instant] = Option(a.attrs.getMTime).map(i => Instant.ofEpochSecond(i.toLong))

  }
}
