package blobstore.sftp

import java.util.Properties
import cats.effect.IO
import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.GenericContainer.FileSystemBind
import com.jcraft.jsch.{JSch, Session}
import org.testcontainers.containers.BindMode

/** sftp-no-chroot-container doesn't map user's home directory to "/". User's instead land in "/home/<username>/"
  */
class SftpStoreNoChrootTest extends AbstractSftpStoreTest {
  override val container: GenericContainer = GenericContainer(
    "atmoz/sftp",
    exposedPorts = List(22),
    command = List("blob:password:::sftp_tests"),
    classpathResourceMapping = List(FileSystemBind("sshd_config", "/etc/ssh/sshd_config", BindMode.READ_ONLY))
  )

  override val session: IO[Session] = IO {
    val jsch = new JSch

    val session = jsch.getSession("blob", container.containerIpAddress, container.mappedPort(22))
    session.setTimeout(10000)
    session.setPassword("password")

    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session // Let the store connect this session
  }
}
