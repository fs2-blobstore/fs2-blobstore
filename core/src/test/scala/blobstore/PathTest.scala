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

import blobstore.url.Path
import org.scalatest.matchers.must.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import cats.data.Chain
import org.scalatest.{Assertion, Inside}
import cats.syntax.all._

class PathTest extends AnyFlatSpec with Matchers with Inside {
  behavior of "Path"
  it should "parse string path to file correctly" in {
    val path = Path("some-bucket/path/to/file")

    path.representation mustBe "some-bucket/path/to/file"
    path.show mustBe "some-bucket/path/to/file"
    path.segments mustBe Chain("some-bucket", "path", "to", "file")
    path.lastSegment mustBe "file".some
    path.plain.show mustBe path.show
    path.plain.representation mustBe path.representation
  }

  it should "parse string path to file without prefix correctly" in {
    val path = Path("some-bucket/path/to/file")

    path.representation mustBe "some-bucket/path/to/file"
    path.show mustBe "some-bucket/path/to/file"
    path.segments mustBe Chain("some-bucket", "path", "to", "file")
    path.lastSegment mustBe "file".some
    path.plain.show mustBe path.show
    path.plain.representation mustBe path.representation
  }

  it should "parse string path to file stating with / correctly" in {
    val path = Path("/some-bucket/path/to/file")

    path.representation mustBe "/some-bucket/path/to/file"
    path.show mustBe "/some-bucket/path/to/file"
    path.segments mustBe Chain("some-bucket", "path", "to", "file")
    path.lastSegment mustBe "file".some
    path.plain.show mustBe path.show
    path.plain.representation mustBe path.representation
  }

  it should "parse string path to dir correctly" in {
    val path = Path("some-bucket/path/to/")

    path.representation mustBe "some-bucket/path/to/"
    path.show mustBe "some-bucket/path/to/"
    path.segments mustBe Chain("some-bucket", "path", "to", "")
    path.lastSegment mustBe "to/".some
    path.plain.show mustBe path.show
    path.plain.representation mustBe path.representation
  }

  it should "parse paths with no key" in {
    val path = Path("some-bucket")

    path.representation mustBe "some-bucket"
    path.show mustBe "some-bucket"
    path.segments mustBe Chain("some-bucket")
    path.lastSegment mustBe "some-bucket".some
    path.plain.show mustBe path.show
    path.plain.representation mustBe path.representation
  }

  it should "parse empty path" in {
    Path("") mustBe Path.empty
  }

  it should "parse paths with spaces" in {
    val path = Path("root with spaces/dir with spaces/file with spaces")

    path.representation mustBe "root with spaces/dir with spaces/file with spaces"
    path.show mustBe "root with spaces/dir with spaces/file with spaces"
    path.segments mustBe Chain("root with spaces", "dir with spaces", "file with spaces")
    path.lastSegment mustBe "file with spaces".some
    path.plain.show mustBe path.show
    path.plain.representation mustBe path.representation
  }

  it should "extend a path with no key correctly" in {
    val path = Path("some-bucket") / "key"

    path.representation mustBe "some-bucket/key"
    path.show mustBe "some-bucket/key"
    path.segments mustBe Chain("some-bucket", "key")
    path.lastSegment mustBe "key".some
    path.plain.show mustBe path.show
    path.plain.representation mustBe path.representation
  }

  it should "be extractable using unapply" in {
    val path  = Path("some-bucket/folder/file").as(())
    val path2 = Path("some-bucket/folder/file")

    inside[Path[Unit], Assertion](path) {
      case Path(s, representation, segments) =>
        s mustBe "some-bucket/folder/file"
        representation mustBe (())
        segments mustBe Chain("some-bucket", "folder", "file")
    }

    inside[Path.Plain, Assertion](path2) {
      case Path(s, representation, segments) =>
        s mustBe "some-bucket/folder/file"
        representation mustBe "some-bucket/folder/file"
        segments mustBe Chain("some-bucket", "folder", "file")
    }
  }
}
