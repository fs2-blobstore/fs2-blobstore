package blobstore.sftp

import java.util.Properties

import cats.effect.IO
import com.jcraft.jsch.JSch

/**
  * sftp-container follows the default atmoz/sftp configuration and will have "/" mapped to the user's home directory
  */
class SftpStoreChrootTest extends AbstractSftpStoreTest {
  override val session = IO {
    val jsch = new JSch()

    val session = jsch.getSession("blob", "sftp-container", 22)
    session.setTimeout(10000)
    session.setPassword("password")

    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)

    session.connect()

    session
  }
}
