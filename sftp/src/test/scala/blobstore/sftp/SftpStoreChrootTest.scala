package blobstore.sftp

import java.util.Properties

import blobstore.url.Authority
import cats.effect.IO
import com.jcraft.jsch.JSch

/**
  * sftp-container follows the default atmoz/sftp configuration and will have "/" mapped to the user's home directory
  */
class SftpStoreChrootTest extends AbstractSftpStoreTest {
  val sftpHost: String = Option(System.getenv("SFTP_HOST")).getOrElse("sftp-container")
  val sftpPort: String = Option(System.getenv("SFTP_CHROOT_PORT")).getOrElse("22")

  override val authority: Authority.Standard = Authority.Standard.unsafe(sftpHost)

  override val session = IO {
    val jsch = new JSch()

    val session = jsch.getSession("blob", sftpHost, sftpPort.toInt)

    session.setTimeout(10000)
    session.setPassword("password")

    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)

    session.connect()

    session
  }
}
