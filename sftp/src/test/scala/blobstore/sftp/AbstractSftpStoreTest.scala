/*
Copyright 2018 LendUp Global, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package blobstore
package sftp

import java.nio.file.Paths

import cats.effect.IO
import cats.effect.concurrent.MVar
import com.jcraft.jsch.{ChannelSftp, Session, SftpException}

import blobstore.implicits._
import cats.implicits._

abstract class AbstractSftpStoreTest extends AbstractStoreTest with PathOps {

  def session: IO[Session]

  private val rootDir                    = Paths.get("tmp/sftp-store-root/").toAbsolutePath.normalize
  protected val mVar                     = MVar.empty[IO, ChannelSftp].unsafeRunSync()
  override lazy val store: SftpStore[IO] = new SftpStore[IO]("", session.unsafeRunSync(), blocker, mVar, None, 10000)
  override val root: String              = "sftp_tests"

  // remove dirs created by AbstractStoreTest
  override def afterAll(): Unit = {
    super.afterAll()

    try {
      store.session.disconnect()
    } catch {
      case _: Throwable =>
    }

    cleanup(rootDir.resolve(s"$root/test-$testRun"))

  }

  behavior of "Sftp store"

  it should "list files in current directory" in {
    val s = session.unsafeRunSync()

    val store: Store[IO] = new SftpStore[IO]("", s, blocker, mVar, None, 10000)
    val path             = Path(".")

    val p = store.list(path).compile.toList

    val result = p.unsafeRunSync()
    result.map(_.filePath) must contain theSameElementsInOrderAs List("sftp_tests")
  }

  it should "list more than 64 (default queue/buffer size) keys" in {

    val dir: Path = dirPath("list-more-than-64")

    val paths = (1 to 256).toList
      .map(i => s"filename-$i.txt")
      .map(writeFile(store, dir))

    val exp = paths.map(_.filePath).toSet

    store.listAll(dir).unsafeRunSync().map(_.filePath).toSet must be(exp)

    val io: IO[List[Unit]] = paths.map(store.remove).sequence
    io.unsafeRunSync()

    store.listAll(dir).unsafeRunSync().isEmpty must be(true)
  }

  it should "be able to remove a directory if it is empty" in {
    val dir      = dirPath("some-dir")
    val filename = "some-filename"

    val result = for {
      file  <- IO(writeFile(store, dir)(filename))
      _     <- store.remove(file)
      _     <- store.remove(dir)
      files <- store.list(dir).compile.toList
    } yield files

    result.unsafeRunSync() must be(List.empty)
  }

  it should "not be able to remove a directory if it is not empty" in {
    val dir      = dirPath("some-dir")
    val filename = "some-filename"

    val failedRemove = for {
      _     <- IO(writeFile(store, dir)(filename))
      _     <- store.remove(dir)
      files <- store.list(dir).compile.toList
    } yield files

    assertThrows[SftpException](failedRemove.unsafeRunSync())
  }

}
