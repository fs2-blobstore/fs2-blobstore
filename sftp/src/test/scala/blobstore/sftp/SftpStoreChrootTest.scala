package blobstore.sftp

import java.util.Properties
import cats.effect.IO
import com.dimafeng.testcontainers.GenericContainer
import com.jcraft.jsch.{JSch, Session}

/** sftp-container follows the default atmoz/sftp configuration and will have "/" mapped to the user's home directory
  */
class SftpStoreChrootTest extends AbstractSftpStoreTest {
  override val container: GenericContainer = GenericContainer(
    "atmoz/sftp",
    exposedPorts = List(22),
    command = List("blob:password:::sftp_tests")
  )

  override val session: IO[Session] = IO {
    val jsch = new JSch()

    val session = jsch.getSession("blob", container.containerIpAddress, container.mappedPort(22))

    session.setTimeout(10000)
    session.setPassword("password")

    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)

    session.connect()

    session
  }
}
