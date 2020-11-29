package blobstore.azure

import java.time.Instant

import blobstore.url.general.GeneralStorageClass
import blobstore.url.FsObject
import com.azure.storage.blob.models.{AccessTier, BlobItemProperties}

case class AzureBlob(
  container: String,
  blob: String,
  properties: Option[BlobItemProperties],
  metadata: Map[String, String]
) extends FsObject {
  override type StorageClassType = AccessTier

  override def name: String = blob

  override def size: Option[Long] = properties.flatMap(bp => Option(bp.getContentLength.longValue()))

  override def isDir: Boolean = properties.isEmpty

  override def lastModified: Option[Instant] = properties.flatMap(bp => Option(bp.getLastModified).map(_.toInstant))

  override def storageClass: Option[AccessTier] = properties.flatMap(bp => Option(bp.getAccessTier))

  override def generalStorageClass: Option[GeneralStorageClass] =
    storageClass.map {
      case AccessTier.ARCHIVE => GeneralStorageClass.ColdStorage
      case _                  => GeneralStorageClass.Standard
    }
}
