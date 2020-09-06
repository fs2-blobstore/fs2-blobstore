///*
//Copyright 2018 LendUp Global, Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
// */
//package blobstore
//
//import java.time.Instant
//
//import org.scalatest.matchers.must.Matchers
//import org.scalatest.flatspec.AnyFlatSpec
//import blobstore.PathOps._
//import cats.data.Chain
//
//import org.scalatest.{Assertion, Inside}
//
//class PathTest extends AnyFlatSpec with Matchers with Inside {
//  behavior of "Path"
//  it should "parse string path to file correctly" in {
//    val s3Path  = Path("s3://some-bucket/path/to/file")
//    val gcsPath = Path("gcs://some-bucket/path/to/file")
//    s3Path mustBe Path(Some("some-bucket"), Chain("path", "to"), Some("file"), None, None, None)
//    gcsPath mustBe Path(Some("some-bucket"), Chain("path", "to"), Some("file"), None, None, None)
//  }
//
//  it should "parse string path to file without prefix correctly" in {
//    val path = Path("some-bucket/path/to/file")
//    path mustBe Path(Some("some-bucket"), Chain("path", "to"), Some("file"), None, None, None)
//  }
//
//  it should "parse string path to file stating with / correctly" in {
//    val path = Path("/some-bucket/path/to/file")
//    path mustBe Path(Some("some-bucket"), Chain("path", "to"), Some("file"), None, None, None)
//  }
//
//  it should "parse string path to dir correctly" in {
//    val path = Path("s3://some-bucket/path/to/")
//    path mustBe Path(Some("some-bucket"), Chain("path", "to"), None, None, None, None)
//  }
//
//  it should "parse paths with no key" in {
//    val path = Path("s3://some-bucket")
//    path mustBe Path(Some("some-bucket"), Chain.empty, None, None, None, None)
//  }
//
//  it should "parse empty path" in {
//    Path("") mustBe BasePath.empty
//  }
//
//  it should "parse paths with spaces" in {
//    Path("root with spaces/dir with spaces/file with spaces") mustBe Path(
//      Some("root with spaces"),
//      Chain("dir with spaces"),
//      Some("file with spaces"),
//      None,
//      None,
//      None
//    )
//  }
//
//  it should "extend a path with no key correctly" in {
//    val path = Path("some-bucket") / "key"
//
//    path mustBe Path(Some("some-bucket"), Chain.empty, Some("key"), None, None, None)
//  }
//
//  it should "replace root if changed resetting dependent fields if necessary" in {
//    val path = BasePath(Some("some-bucket"), Chain("folder"), Some("file"), Some(10), Some(false), Some(Instant.EPOCH))
//
//    val changed = path.withRoot(Some("another-bucket"), reset = false)
//    changed.root must contain("another-bucket")
//    changed.size must contain(10)
//    changed.isDir must contain(false)
//    changed.lastModified must contain(Instant.EPOCH)
//
//    path.withRoot(None, reset = false).root mustBe empty
//
//    path must be theSameInstanceAs path.withRoot(Some("some-bucket"), reset = false)
//    path must be theSameInstanceAs path.withRoot(Some("some-bucket"))
//
//    val reset = path.withRoot(Some("different-bucket"))
//    reset.root must contain("different-bucket")
//    (reset.pathFromRoot -> reset.fileName) mustBe (path.pathFromRoot -> path.fileName)
//    reset.size mustBe empty
//    reset.isDir mustBe empty
//    reset.lastModified mustBe empty
//  }
//
//  it should "replace path from root if changed resetting dependent fields if necessary" in {
//    val path = BasePath(Some("some-bucket"), Chain("folder"), Some("file"), Some(10), Some(false), Some(Instant.EPOCH))
//
//    val changed = path.withPathFromRoot(Chain("another-folder", "subfolder"), reset = false)
//    changed.pathFromRoot mustBe Chain("another-folder", "subfolder")
//    changed.size must contain(10)
//    changed.isDir must contain(false)
//    changed.lastModified must contain(Instant.EPOCH)
//
//    path.withPathFromRoot(Chain.empty, reset = false).pathFromRoot mustBe Chain.empty
//
//    path must be theSameInstanceAs path.withPathFromRoot(Chain("folder"), reset = false)
//    path must be theSameInstanceAs path.withPathFromRoot(Chain("folder"))
//
//    val reset = path.withPathFromRoot(Chain("different-folder", "subfolder"))
//    reset.pathFromRoot mustBe Chain("different-folder", "subfolder")
//    (reset.root -> reset.fileName) mustBe (path.root -> path.fileName)
//    reset.size mustBe empty
//    reset.isDir mustBe empty
//    reset.lastModified mustBe empty
//  }
//
//  it should "replace filename if changed resetting dependent fields if necessary" in {
//    val path = BasePath(Some("some-bucket"), Chain("folder"), Some("file"), Some(10), Some(false), Some(Instant.EPOCH))
//
//    val changed = path.withFileName(Some("another-file"), reset = false)
//    changed.fileName must contain("another-file")
//    changed.size must contain(10)
//    changed.isDir must contain(false)
//    changed.lastModified must contain(Instant.EPOCH)
//
//    path.withFileName(None, reset = false).fileName mustBe empty
//
//    path must be theSameInstanceAs path.withFileName(Some("file"), reset = false)
//    path must be theSameInstanceAs path.withFileName(Some("file"))
//
//    val reset = path.withFileName(Some("different-file"))
//    reset.fileName must contain("different-file")
//    (reset.root -> reset.pathFromRoot) mustBe (path.root -> path.pathFromRoot)
//    reset.size mustBe empty
//    reset.isDir mustBe empty
//    reset.lastModified mustBe empty
//  }
//
//  it should "replace size if changed resetting dependent fields if necessary" in {
//    val path = BasePath(Some("some-bucket"), Chain("folder"), Some("file"), Some(10), Some(false), Some(Instant.EPOCH))
//
//    val changed = path.withSize(Some(20), reset = false)
//    changed.size must contain(20)
//    changed.isDir must contain(false)
//    changed.lastModified must contain(Instant.EPOCH)
//
//    path.withSize(None, reset = false).size mustBe empty
//    path must be theSameInstanceAs path.withSize(Some(10), reset = false)
//    path must be theSameInstanceAs path.withSize(Some(10))
//
//    val reset = path.withSize(Some(50))
//    (reset.root, reset.pathFromRoot, reset.fileName) mustBe ((path.root, path.pathFromRoot, path.fileName))
//    reset.size must contain(50)
//    reset.isDir mustBe empty
//    reset.lastModified mustBe empty
//  }
//
//  it should "replace isDir if changed resetting dependent fields if necessary" in {
//    val path = BasePath(Some("some-bucket"), Chain("folder"), Some("file"), Some(10), Some(false), Some(Instant.EPOCH))
//
//    val changed = path.withIsDir(Some(true), reset = false)
//    changed.size must contain(10)
//    changed.isDir must contain(true)
//    changed.lastModified must contain(Instant.EPOCH)
//
//    path.withIsDir(None, reset = false).isDir mustBe empty
//    path must be theSameInstanceAs path.withIsDir(Some(false), reset = false)
//    path must be theSameInstanceAs path.withIsDir(Some(false))
//
//    val reset = path.withIsDir(Some(true))
//    (reset.root, reset.pathFromRoot, reset.fileName) mustBe ((path.root, path.pathFromRoot, path.fileName))
//    reset.size mustBe empty
//    reset.isDir must contain(true)
//    reset.lastModified mustBe empty
//  }
//
//  it should "replace lastModified if changed resetting dependent fields if necessary" in {
//    val path = BasePath(Some("some-bucket"), Chain("folder"), Some("file"), Some(10), Some(false), Some(Instant.EPOCH))
//
//    val changed = path.withLastModified(Some(Instant.MIN), reset = false)
//    changed.size must contain(10)
//    changed.isDir must contain(false)
//    changed.lastModified must contain(Instant.MIN)
//
//    path.withLastModified(None, reset = false).lastModified mustBe empty
//    path must be theSameInstanceAs path.withLastModified(Some(Instant.EPOCH), reset = false)
//    path must be theSameInstanceAs path.withLastModified(Some(Instant.EPOCH))
//
//    val reset = path.withLastModified(Some(Instant.MIN))
//    (reset.root, reset.pathFromRoot, reset.fileName) mustBe ((path.root, path.pathFromRoot, path.fileName))
//    reset.size mustBe empty
//    reset.isDir mustBe empty
//    reset.lastModified must contain(Instant.MIN)
//  }
//
//  it should "be extractable using unapply" in {
//    val path =
//      Path("some-bucket/folder/file")
//        .withSize(Some(0), reset = false)
//        .withIsDir(Some(true), reset = false)
//        .withLastModified(Some(Instant.EPOCH), reset = false)
//    inside[Path, Assertion](path) {
//      case Path(root, pathFromRoot, fileName, size, isDir, lastModified) =>
//        root must contain("some-bucket")
//        pathFromRoot mustBe Chain("folder")
//        fileName must contain("file")
//        size must contain(0)
//        isDir must contain(true)
//        lastModified must contain(Instant.EPOCH)
//    }
//  }
//}
