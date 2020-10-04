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

import blobstore.url.{Authority, Path}
import cats.effect.IO
import cats.effect.concurrent.MVar
import com.jcraft.jsch.{ChannelSftp, Session, SftpException}

abstract class AbstractSftpStoreTest extends AbstractStoreTest[Authority.Standard, SftpFile] {

  def session: IO[Session]
  override lazy val testRunRoot: Path.Plain = Path(s"sftp_tests/test-$testRun")
  override val fileSystemRoot               = Path("sftp_tests")

  private val rootDir = Paths.get("tmp/sftp-store-root/").toAbsolutePath.normalize
  protected val mVar  = MVar.empty[IO, ChannelSftp].unsafeRunSync()
  lazy val sftpStore: SftpStore[IO] =
    SftpStore[IO](session, blocker).compile.resource.lastOrError.allocated.map(_._1).unsafeRunSync()
  override lazy val store: Store[IO, Authority.Standard, SftpFile] = sftpStore.liftTo[Authority.Standard]

  val scheme = "sftp"

  // remove dirs created by AbstractStoreTest
  override def afterAll(): Unit = {
    super.afterAll()

    try {
      sftpStore.session.disconnect()
    } catch {
      case _: Throwable =>
    }

    cleanup(rootDir.resolve(s"$authority/test-$testRun"))

  }

  behavior of "Sftp store"

  it should "list files in current directory" in {
    val store =
      SftpStore[IO](session, blocker).compile.resource.lastOrError.allocated.map(_._1).unsafeRunSync()
    val path = Path(".")

    val p = store.list(path).compile.toList

    val result = p.unsafeRunSync()
    result.map(_.lastSegment.getOrElse("")) must contain theSameElementsInOrderAs List("sftp_tests/")
  }

  it should "list more than 64 (default queue/buffer size) keys" in {

    val dir = dirUrl("list-more-than-64")

    val paths = (1 to 256).toList
      .map(i => s"filename-$i.txt")
      .map(writeFile(store, dir.path))

    val exp = paths.map(_.path.lastSegment).toSet

    store.list(dir).compile.toList.unsafeRunSync().map(_.lastSegment).toSet mustBe exp

    store.remove(dir, recursive = true).unsafeRunSync()

    store.list(dir).compile.toList.unsafeRunSync().isEmpty mustBe true
  }

  it should "be able to remove a directory if it is empty" in {
    val dir      = dirUrl("some-dir")
    val filename = "some-filename"

    val result = for {
      file  <- IO(writeFile(store, dir.path)(filename))
      _     <- store.remove(file, recursive = false)
      _     <- store.remove(dir, recursive = false)
      files <- store.list(dir).compile.toList
    } yield files

    result.unsafeRunSync() must be(List.empty)
  }

  it should "not be able to remove a directory if it is not empty" in {
    val dir      = dirUrl("some-dir")
    val filename = "some-filename"

    val failedRemove = for {
      _     <- IO(writeFile(store, dir.path)(filename))
      _     <- store.remove(dir, recursive = false)
      files <- store.list(dir).compile.toList
    } yield files

    assertThrows[SftpException](failedRemove.unsafeRunSync())
  }

}
