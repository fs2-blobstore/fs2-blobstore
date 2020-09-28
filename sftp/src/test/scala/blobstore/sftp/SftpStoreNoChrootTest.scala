package blobstore.sftp

import java.nio.charset.StandardCharsets
import java.util.Properties

import blobstore.Store
import blobstore.url.Path
import blobstore.url.Path.Plain
import cats.effect.IO
import fs2.Stream
import com.jcraft.jsch.{ChannelSftp, JSch, Session}

/**
  * sftp-no-chroot-container doesn't map user's home directory to "/". User's instead land in "/home/<username>/"
  */
class SftpStoreNoChrootTest extends AbstractSftpStoreTest {

  override val session: IO[Session] = IO {
    val jsch = new JSch()

    val session = jsch.getSession("blob", "sftp-no-chroot", 22)
    session.setTimeout(10000)
    session.setPassword("password")

    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session // Let the store connect this session
  }

}
