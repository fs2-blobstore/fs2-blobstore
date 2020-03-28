package blobstore
package s3

import java.util.Date

import com.amazonaws.services.s3.model.S3ObjectSummary

class S3PathTest extends AbstractPathTest[S3Path] {
  val summary = {
    val s = new S3ObjectSummary()
    s.setBucketName(root)
    s.setKey(dir ++ "/" ++ fileName)
    s.setSize(fileSize)
    s.setLastModified(Date.from(lastModified))
    s
  }
  val s3File = S3Path.S3File(summary, None)

  val concretePath = new S3Path(Right(s3File), knownSize = true)
}
