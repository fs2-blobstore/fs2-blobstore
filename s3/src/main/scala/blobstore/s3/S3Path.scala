package blobstore
package s3

import java.time.Instant

import cats.data.Chain
import com.amazonaws.services.s3.model.{ObjectMetadata, S3ObjectSummary}

class S3Path private[s3] (val s3Entity: Either[S3Path.S3Dir, S3Path.S3File], private[s3] val knownSize: Boolean = false)
  extends Path {
  private val maybeFlatName =
    s3Entity.fold(_ => None, s3File => Option(s3File.summary.getKey))

  val root: Option[String] = Option(s3Entity.fold(_.bucket, _.summary.getBucketName))
  val pathFromRoot: Chain[String] = s3Entity.fold(
    _.path,
    _ =>
      Chain.fromSeq(maybeFlatName.fold(Array.empty[String])(_.split('/').filter(_.nonEmpty).dropRight(1)).toIndexedSeq)
  )
  val fileName: Option[String] = s3Entity.fold(
    _ => None,
    _ =>
      maybeFlatName.flatMap { flatName =>
        flatName.split('/').filter(_.nonEmpty).lastOption.map(_ ++ (if (flatName.endsWith("/")) "/" else ""))
      }
  )
  val size: Option[Long] = s3Entity.fold(
    _ => None,
    s3File => if (s3File.summary.getSize != 0 || knownSize) Some(s3File.summary.getSize) else None
  )
  val isDir: Option[Boolean] = s3Entity.fold(_ => Some(true), _ => Some(false))
  val lastModified: Option[Instant] =
    s3Entity.fold(_ => None, s3File => Option(s3File.summary.getLastModified).map(_.toInstant))
}

object S3Path {
  final case class S3Dir(bucket: String, path: Chain[String])
  object S3Dir {
    def apply(bucket: String, prefix: String): S3Dir =
      new S3Dir(bucket, Chain.fromSeq(prefix.split('/').filter(_.nonEmpty).toIndexedSeq))
  }
  final case class S3File(summary: S3ObjectSummary, metadata: Option[ObjectMetadata])
  object S3File {
    def summaryFromMetadata(bucket: String, key: String, meta: ObjectMetadata): S3ObjectSummary = {
      val summary = new S3ObjectSummary()
      summary.setBucketName(bucket)
      summary.setKey(key)
      Option(meta.getETag).foreach(summary.setETag)
      Option(meta.getLastModified).foreach(summary.setLastModified)
      Option(meta.getContentLength).foreach(summary.setSize)
      Option(meta.getStorageClass).foreach(summary.setStorageClass)
      summary
    }
  }

  def apply(s3Entity: Either[S3Dir, S3File]): S3Path = new S3Path(s3Entity, false)

  def unapply(s3Path: S3Path): Option[Either[S3Dir, S3File]] = Some(s3Path.s3Entity)

  def narrow(p: Path): Option[S3Path] = p match {
    case s3Path: S3Path => Some(s3Path)
    case _              => None
  }
}
