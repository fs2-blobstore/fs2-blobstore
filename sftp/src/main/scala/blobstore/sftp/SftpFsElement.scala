package blobstore.sftp

import java.time.Instant

import blobstore.url.FileSystemObject
import blobstore.url.general.GeneralFileSystemObject
import com.jcraft.jsch.SftpATTRS

case class SftpFsElement(name: String, attrs: SftpATTRS)

object SftpFsElement {
  def toGeneral(blob: SftpFsElement): GeneralFileSystemObject = {

    val fso = FileSystemObject[SftpFsElement]
    GeneralFileSystemObject(
      name = fso.name(blob),
      size = fso.size(blob),
      isDir = fso.isDir(blob),
      storageClass = None,
      lastModified = fso.lastModified(blob)
    )
  }

  implicit val blob: FileSystemObject[SftpFsElement] = new FileSystemObject[SftpFsElement] {
    override def name(a: SftpFsElement): String = a.name

    override def size(a: SftpFsElement): Option[Long] = Option(a.attrs.getSize)

    override def isDir(a: SftpFsElement): Boolean = a.attrs.isDir

    override def lastModified(a: SftpFsElement): Option[Instant] = Option(a.attrs.getMTime).map(i => Instant.ofEpochSecond(i.toLong))
  }
}
