//package blobstore
//package s3
//
//import java.time.Instant
//
//import cats.data.Chain
//
//case class S3Path(
//  bucket: String,
//  key: String,
//  meta: Option[S3MetaInfo]
//) extends Path {
//
//  val root: Option[String] = Some(bucket)
//  val pathFromRoot: Chain[String] = {
//    val split = key.split('/').filter(_.nonEmpty)
//    Chain.fromSeq(meta.fold(split)(_ => split.dropRight(1)).toIndexedSeq)
//  }
//
//  val fileName: Option[String] =
//    meta.fold(Option.empty[String])(_ =>
//      key.split('/').filter(_.nonEmpty).lastOption.map(_ ++ (if (key.endsWith("/")) "/" else ""))
//    )
//
//  val size: Option[Long] = meta.flatMap(_.size)
//
//  val isDir: Option[Boolean] = Some(meta.fold(true)(_ => false))
//  val lastModified: Option[Instant] =
//    meta.flatMap(_.lastModified)
//}
//
//object S3Path {
//  def narrow(p: Path): Option[S3Path] = p match {
//    case s3Path: S3Path => Some(s3Path)
//    case _              => None
//  }
//}
