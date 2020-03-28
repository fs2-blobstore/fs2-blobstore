package blobstore
package s3

class S3PathTest extends AbstractPathTest[S3Path] {

  val concretePath = new S3Path(
    root,
    dir ++ "/" ++ fileName,
    Some(S3MetaInfo.const(constSize = Some(fileSize), constLastModified = Some(lastModified)))
  )
}
