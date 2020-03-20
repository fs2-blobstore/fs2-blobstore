package blobstore.box

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import blobstore.BasePath
import cats.data.Chain
import com.box.sdk.BoxFile
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import cats.syntax.either._

class BoxPathTest extends AnyFlatSpec with Matchers {
  behavior of "BoxPath"

  it should "be converted to BasePath on all changes except no-op" in {
    @SuppressWarnings(Array("scalafix:DisableSyntax.null"))
    val file = new BoxFile(null, "")
    val json =
      """{
        |  "id": "fileId",
        |  "type": "file",
        |  "name": "filename",
        |  "size": 20,
        |  "modified_at": "2020-03-23T00:00:00-00",
        |  "path_collection": {
        |    "total_count": 2,
        |    "entries": [
        |      {
        |        "id": "rootId",
        |        "name": "root"
        |      },
        |      {
        |        "id": "folderId",
        |        "name": "subdir"
        |      }
        |    ]
        |  }
        |}""".stripMargin
    val fileInfo: BoxFile#Info = new file.Info(json)
    val boxPath                = new BoxPath(Some("root"), Some("rootId"), fileInfo.asLeft)
    val instant                = ZonedDateTime.of(2020, 3, 23, 0, 0, 0, 0, ZoneOffset.UTC).toInstant

    boxPath.root must contain("root")
    boxPath.pathFromRoot mustBe Chain("subdir")
    boxPath.fileName must contain("filename")
    boxPath.size must contain(20)
    boxPath.isDir must contain(false)
    boxPath.lastModified must contain(instant)

    boxPath.withRoot(Some("root")) mustBe a[BoxPath]
    boxPath.withRoot(Some("root")) mustBe theSameInstanceAs(boxPath)
    boxPath.withPathFromRoot(Chain("subdir")) mustBe a[BoxPath]
    boxPath.withPathFromRoot(Chain("subdir")) mustBe theSameInstanceAs(boxPath)
    boxPath.withFileName(Some("filename")) mustBe a[BoxPath]
    boxPath.withFileName(Some("filename")) mustBe theSameInstanceAs(boxPath)
    boxPath.withSize(Some(20)) mustBe a[BoxPath]
    boxPath.withSize(Some(20)) mustBe theSameInstanceAs(boxPath)
    boxPath.withIsDir(Some(false)) mustBe a[BoxPath]
    boxPath.withIsDir(Some(false)) mustBe theSameInstanceAs(boxPath)
    boxPath.withLastModified(Some(instant)) mustBe a[BoxPath]
    boxPath.withLastModified(Some(instant)) mustBe theSameInstanceAs(boxPath)

    boxPath.withRoot(Some("different-root")) mustBe BasePath(
      Some("different-root"),
      Chain("subdir"),
      Some("filename"),
      None,
      None,
      None
    )
    boxPath.withPathFromRoot(Chain("different", "subdir")) mustBe BasePath(
      Some("root"),
      Chain("different", "subdir"),
      Some("filename"),
      None,
      None,
      None
    )
    boxPath.withFileName(Some("different-filename")) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("different-filename"),
      None,
      None,
      None
    )
    boxPath.withSize(Some(10)) mustBe BasePath(Some("root"), Chain("subdir"), Some("filename"), Some(10), None, None)
    boxPath.withIsDir(Some(true)) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      None,
      Some(true),
      None
    )
    boxPath.withLastModified(Some(Instant.EPOCH)) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      None,
      None,
      Some(Instant.EPOCH)
    )

    boxPath.withRoot(Some("different-root"), reset = false) mustBe BasePath(
      Some("different-root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(instant)
    )
    boxPath.withPathFromRoot(Chain("different", "subdir"), reset = false) mustBe BasePath(
      Some("root"),
      Chain("different", "subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(instant)
    )
    boxPath.withFileName(Some("different-filename"), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("different-filename"),
      Some(20),
      Some(false),
      Some(instant)
    )
    boxPath.withSize(Some(10), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(10),
      Some(false),
      Some(instant)
    )
    boxPath.withIsDir(Some(true), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(true),
      Some(instant)
    )
    boxPath.withLastModified(Some(Instant.EPOCH), reset = false) mustBe BasePath(
      Some("root"),
      Chain("subdir"),
      Some("filename"),
      Some(20),
      Some(false),
      Some(Instant.EPOCH)
    )
  }
}
