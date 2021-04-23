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
package fs

import blobstore.url.{Authority, Path, Url}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

class FileStoreTest extends AbstractStoreTest[NioPath] {

  private val localStore: FileStore[IO] = FileStore[IO]

  override def mkStore(): Store[IO, NioPath] =
    localStore.lift((u: Url.Plain) => u.path.valid)

  override val scheme: String       = "file"
  override val authority: Authority = Authority.localhost

  override val fileSystemRoot: Path.Plain   = testRunRoot
  override lazy val testRunRoot: Path.Plain = Path(s"/tmp/fs2blobstore/filestore/$testRun/")

  behavior of "FileStore.put"

  it should "pick up correct storage class" in {
    val dir     = dirUrl("trailing-slash")
    val fileUrl = dir / "file"

    store.putContent(fileUrl, "test").unsafeRunSync()

    store.list(fileUrl).map { u =>
      u.path.storageClass mustBe None
    }.compile.lastOrError.unsafeRunSync()

    val storeGeneric: Store.Generic[IO] = store

    storeGeneric.list(fileUrl).map { u =>
      u.path.storageClass mustBe None
    }.compile.lastOrError.unsafeRunSync()
  }

  it should "not have side effects when creating a Sink" in {
    localStore.put(Path(s"fs_tests_$testRun/path/file.txt"))
    localStore.list(Path(s"fs_tests_$testRun/")).compile.toList.unsafeRunSync() mustBe Nil
  }
}
