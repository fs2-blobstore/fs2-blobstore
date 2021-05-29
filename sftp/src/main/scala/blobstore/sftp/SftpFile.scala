package blobstore.sftp

import java.time.Instant
import blobstore.url.{FsObject, FsObjectLowPri}
import blobstore.url.general.{GeneralStorageClass, StorageClassLookup}
import com.jcraft.jsch.SftpATTRS

case class SftpFile(name: String, attrs: SftpATTRS) extends FsObject {
  override type StorageClassType = Nothing

  override def size: Option[Long] = Option(attrs.getSize)

  override def isDir: Boolean = attrs.isDir

  override def lastModified: Option[Instant] = Option(attrs.getMTime).map(i => Instant.ofEpochSecond(i.toLong))

  override private[blobstore] def generalStorageClass: Option[GeneralStorageClass] = None
}

object SftpFile extends FsObjectLowPri {
  implicit val storageClassLookup: StorageClassLookup.Aux[SftpFile, Nothing] = new StorageClassLookup[SftpFile] {
    override type StorageClassType = Nothing

    override def storageClass(a: SftpFile): None.type = None
  }
}
