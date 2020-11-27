package blobstore.sftp

import java.time.Instant

import blobstore.url.FileSystemObject
import blobstore.url.general.UniversalFileSystemObject
import com.jcraft.jsch.SftpATTRS

case class SftpFile(name: String, attrs: SftpATTRS)

object SftpFile {

  implicit val blob: FileSystemObject[SftpFile] = new FileSystemObject[SftpFile] {
    type StorageClassType = Nothing

    override def name(a: SftpFile): String = a.name

    override def size(a: SftpFile): Option[Long] = Option(a.attrs.getSize)

    override def isDir(a: SftpFile): Boolean = a.attrs.isDir

    override def lastModified(a: SftpFile): Option[Instant] =
      Option(a.attrs.getMTime).map(i => Instant.ofEpochSecond(i.toLong))

    override def storageClass(a: SftpFile): Option[StorageClassType] = None

    override def universal(a: SftpFile): UniversalFileSystemObject =
      UniversalFileSystemObject(
        name(a),
        size(a),
        isDir(a),
        storageClass(a),
        lastModified(a)
      )
  }
}
