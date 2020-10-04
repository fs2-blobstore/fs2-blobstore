package blobstore.sftp

import java.util.Properties

import cats.effect.IO
import com.jcraft.jsch.{JSch, Session}

/**
  * sftp-no-chroot-container doesn't map user's home directory to "/". User's instead land in "/home/<username>/"
  */
class SftpStoreNoChrootTest extends AbstractSftpStoreTest {

  override val session: IO[Session] = IO {
    val jsch = new JSch()

    val sftpHost: String = Option(System.getenv("SFTP_HOST")).getOrElse("sftp-no-chroot")
    val sftpPort: String = Option(System.getenv("SFTP_PORT")).getOrElse("22")

    val session = jsch.getSession("blob", sftpHost, sftpPort.toInt)
    session.setTimeout(10000)
    session.setPassword("password")

    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session // Let the store connect this session
  }

}
