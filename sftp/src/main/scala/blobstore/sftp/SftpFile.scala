package blobstore.sftp

import java.time.Instant

import blobstore.url.FsObject
import blobstore.url.general.GeneralStorageClass
import com.jcraft.jsch.SftpATTRS

case class SftpFile(name: String, attrs: SftpATTRS) extends FsObject {
  override type StorageClassType = Nothing

  override def size: Option[Long] = Option(attrs.getSize)

  override def isDir: Boolean = attrs.isDir

  override def lastModified: Option[Instant] = Option(attrs.getMTime).map(i => Instant.ofEpochSecond(i.toLong))

  override def storageClass: Option[Nothing] = None

  override def generalStorageClass: Option[GeneralStorageClass] = None
}
