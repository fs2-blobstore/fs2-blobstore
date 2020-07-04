package blobstore.sftp

import java.nio.charset.StandardCharsets
import java.util.Properties

import blobstore.Store
import cats.effect.IO
import fs2.Stream
import com.jcraft.jsch.{ChannelSftp, JSch, Session}

/**
  * sftp-no-chroot-container doesn't map user's home directory to "/". User's instead land in "/home/<username>/"
  */
class SftpStoreNoChrootTest extends AbstractSftpStoreTest {
  override val session: IO[Session] = IO {
    val jsch = new JSch()

    val session = jsch.getSession("blob", "sftp-no-chroot-container", 22)
    session.setTimeout(10000)
    session.setPassword("password")

    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session // Let the store connect this session
  }

  it should "honor absRoot on put" in {
    val s = session.unsafeRunSync()
    s.connect(10000)

    val store: Store[IO] =
      new SftpStore[IO](s"/home/blob/", s, blocker, mVar, None, 10000)

    val filePath = dirPath("baz/bam") / "tmp"

    val save = Stream
      .emits("foo".getBytes(StandardCharsets.UTF_8))
      .covary[IO]
      .through(store.put(filePath))
      .compile
      .toList

    val storeRead = store.get(filePath, 1024).through(fs2.text.utf8Decode).compile.toList
    val is = IO {
      @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
      val ch = s.openChannel("sftp").asInstanceOf[ChannelSftp]
      ch.connect()
      ch.get(s"/home/blob/$filePath")
    }
    val directRead = fs2.io.readInputStream(is, 1024, blocker).through(fs2.text.utf8Decode).compile.toList

    val program = for {
      _                 <- save
      contentsFromStore <- storeRead
      contentsDirect    <- directRead
    } yield contentsDirect.mkString("\n") -> contentsFromStore.mkString("\n")

    val (fromStore, fromDirect) = program.unsafeRunSync()
    fromStore mustBe "foo"
    fromDirect mustBe "foo"
  }
}
