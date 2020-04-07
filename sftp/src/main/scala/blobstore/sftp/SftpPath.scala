package blobstore.sftp

import java.time.Instant

import blobstore.Path
import cats.data.Chain
import com.jcraft.jsch.SftpATTRS

case class SftpPath(root: Option[String], fileName: Option[String], pathFromRoot: Chain[String], attributes: SftpATTRS)
  extends Path {

  override val isDir: Option[Boolean] = Option(attributes.isDir)

  override val size: Option[Long] = Option(attributes.getSize)

  override val lastModified: Option[Instant] = Option(attributes.getMTime.toLong).map(Instant.ofEpochSecond)
}
