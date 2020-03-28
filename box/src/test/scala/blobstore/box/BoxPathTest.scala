package blobstore.box

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import blobstore.AbstractPathTest
import com.box.sdk.BoxFile
import cats.syntax.either._

class BoxPathTest extends AbstractPathTest[BoxPath] {
  @SuppressWarnings(Array("scalafix:DisableSyntax.null"))
  val file = new BoxFile(null, "")
  val dtf  = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
  val json =
    s"""{
       |  "id": "fileId",
       |  "type": "file",
       |  "name": "$fileName",
       |  "size": $fileSize,
       |  "modified_at": "${dtf.format(lastModified.atZone(ZoneOffset.UTC))}",
       |  "path_collection": {
       |    "total_count": 2,
       |    "entries": [
       |      {
       |        "id": "rootId",
       |        "name": "$root"
       |      },
       |      {
       |        "id": "folderId",
       |        "name": "$dir"
       |      }
       |    ]
       |  }
       |}""".stripMargin
  val fileInfo: BoxFile#Info = new file.Info(json)
  val concretePath           = new BoxPath(Some(root), Some("rootId"), fileInfo.asLeft)
}
