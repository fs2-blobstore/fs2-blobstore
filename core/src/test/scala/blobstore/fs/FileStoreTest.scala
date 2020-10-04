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

import blobstore.url.{Authority, Path}
import cats.effect.IO

class FileStoreTest extends AbstractStoreTest[Authority.Standard, NioPath] {

  override lazy val testRunRoot: Path.Plain = Path(s"/tmp/fs2blobstore/filestore/$testRun/")

  override val fileSystemRoot: Path.Plain = testRunRoot
  private val localStore: FileStore[IO] = FileStore[IO](blocker)
  override val store: Store[IO, Authority.Standard, NioPath] = localStore.liftTo[Authority.Standard, NioPath](identity)
  override val authority: Authority.Standard     = Authority.Standard.localhost
  override val scheme: String = "file"

  behavior of "FileStore.put"
  it should "not have side effects when creating a Sink" in {
    localStore.put(Path(s"fs_tests_$testRun/path/file.txt"))
    localStore.list(Path(s"fs_tests_$testRun/")).compile.toList.unsafeRunSync().isEmpty must be(true)
  }
}
