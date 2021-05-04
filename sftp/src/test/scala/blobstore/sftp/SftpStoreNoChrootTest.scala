package blobstore.sftp

import java.util.Properties
import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.GenericContainer
import com.jcraft.jsch.{JSch, Session}
import org.testcontainers.containers.BindMode
import weaver.GlobalRead

/** sftp-no-chroot-container doesn't map user's home directory to "/". User's instead land in "/home/<username>/"
  */
class SftpStoreNoChrootTest(global: GlobalRead) extends AbstractSftpStoreTest(global) {
  override val container: GenericContainer = GenericContainer(
    "atmoz/sftp",
    exposedPorts = List(22),
    command = List("blob:password:::sftp_tests"),
    classpathResourceMapping = List(("sshd_config", "/etc/ssh/sshd_config", BindMode.READ_ONLY))
  )

  override def sessionResource: Resource[IO, Session] =
    Resource.make(IO.blocking(container.start()))(_ => IO.blocking(container.stop())).map { _ =>
      val jsch    = new JSch()
      val session = jsch.getSession("blob", container.containerIpAddress, container.mappedPort(22))
      session.setTimeout(10000)
      session.setPassword("password")
      val config = new Properties
      config.put("StrictHostKeyChecking", "no")
      session.setConfig(config)
      session
    }

}
