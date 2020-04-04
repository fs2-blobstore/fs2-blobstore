package blobstore.azure

import java.time.Instant

import blobstore.Path
import cats.data.Chain
import com.azure.storage.blob.models.BlobItemProperties

case class AzurePath(container: String, blob: String, properties: Option[BlobItemProperties], meta: Map[String, String])
  extends Path {
  val root: Option[String]   = Some(container)
  val isDir: Option[Boolean] = Some(properties.isEmpty)
  val pathFromRoot: Chain[String] = {
    val segments = {
      val split = blob.split('/').filter(_.nonEmpty)
      if (isDir.contains(true)) split else split.dropRight(1)
    }
    Chain.fromSeq(segments.toIndexedSeq)
  }
  val fileName: Option[String] =
    if (isDir.contains(true)) None
    else {
      blob.split('/').filter(_.nonEmpty).lastOption.map(_ ++ (if (blob.endsWith("/")) "/" else ""))
    }
  val size: Option[Long] =
    properties.flatMap(bip => Option(bip.getContentLength).map(_.toLong))
  val lastModified: Option[Instant] =
    properties.flatMap(bp => Option(bp.getLastModified).map(_.toInstant))
}

object AzurePath {
  def narrow(p: Path): Option[AzurePath] = p match {
    case azureP: AzurePath => Some(azureP)
    case _                 => None
  }
}
