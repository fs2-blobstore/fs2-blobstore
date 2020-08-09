package blobstore.sftp

import java.time.Instant

import blobstore.experiment.url.Blob
import blobstore.experiment.url.Url.PlainUrl
import blobstore.url.Blob
import com.jcraft.jsch.SftpATTRS

package object experiment {

  implicit val blob: Blob[SftpATTRS] = new Blob[SftpATTRS] {
    override def size(a: SftpATTRS): Long = a.getSize

    override def isDir(a: SftpATTRS): Boolean = a.isDir

    override def lastModified(a: SftpATTRS): Option[Instant] = Option(a.getMTime).map(Instant.ofEpochSecond)
  }
}
