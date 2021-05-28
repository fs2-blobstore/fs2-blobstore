package blobstore.azure

import java.time.Instant
import blobstore.url.general.{GeneralStorageClass, StorageClassLookup}
import blobstore.url.{FsObject, FsObjectLowPri}
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

  override private[blobstore] def generalStorageClass: Option[GeneralStorageClass] =
    storageClass.map {
      case AccessTier.ARCHIVE => GeneralStorageClass.ColdStorage
      case _                  => GeneralStorageClass.Standard
    }
}

object AzureBlob extends FsObjectLowPri {
  implicit val storageClassLookup: StorageClassLookup.Aux[AzureBlob, AccessTier] = new StorageClassLookup[AzureBlob] {
    override type StorageClassType = AccessTier

    override def storageClass(a: AzureBlob): Option[AccessTier] = a.properties.flatMap(bp => Option(bp.getAccessTier))
  }
}
