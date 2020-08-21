//package blobstore
//package gcs
//
//import java.time.Instant
//
//import cats.data.Chain
//import com.google.cloud.storage.BlobInfo
//
//case class GcsPath(blobInfo: BlobInfo) extends Path {
//  private val maybeFlatName = Option(blobInfo.getName)
//
//  val root: Option[String]   = Option(blobInfo.getBucket)
//  val isDir: Option[Boolean] = Option(blobInfo.isDirectory)
//  val size: Option[Long]     = Option(blobInfo.getSize: java.lang.Long).map(_.toLong)
//  val lastModified: Option[Instant] =
//    Option(blobInfo.getUpdateTime: java.lang.Long).map(millis => Instant.ofEpochMilli(millis))
//  val fileName: Option[String] =
//    if (isDir.contains(true)) None
//    else
//      maybeFlatName.flatMap(flatName =>
//        flatName.split('/').filter(_.nonEmpty).lastOption.map(_ ++ (if (flatName.endsWith("/")) "/" else ""))
//      )
//
//  val pathFromRoot: Chain[String] = {
//    val segments = maybeFlatName.fold(Array.empty[String]) { flatName =>
//      val split = flatName.split('/').filter(_.nonEmpty)
//      if (isDir.contains(true)) split else split.dropRight(1)
//    }
//    Chain.fromSeq(segments.toIndexedSeq)
//  }
//}
//
//object GcsPath {
//  def narrow(p: Path): Option[GcsPath] = p match {
//    case gcsP: GcsPath => Some(gcsP)
//    case _             => None
//  }
//}
