package blobstore.sftp

import java.util.Properties

import cats.effect.IO
import com.jcraft.jsch.JSch

/**
  * sftp-no-chroot-container doesn't map user's home directory to "/". User's instead land in "/home/<username>/"
  */
class SftpStoreNoChrootTest extends AbstractSftpStoreTest {
  override val session = IO {
    val jsch = new JSch()

    val session = jsch.getSession("blob", "sftp-no-chroot", 22)
    session.setTimeout(10000)
    session.setPassword("password")

    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)

    session.connect()

    session
  }
}
