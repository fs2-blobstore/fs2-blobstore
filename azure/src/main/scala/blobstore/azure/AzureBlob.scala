package blobstore.azure

import java.time.Instant
import blobstore.url.FileSystemObject
import blobstore.url.general.{GeneralStorageClass, UniversalFileSystemObject}
import com.azure.storage.blob.models.{AccessTier, BlobItemProperties}

case class AzureBlob(
  container: String,
  blob: String,
  properties: Option[BlobItemProperties],
  metadata: Map[String, String]
)

object AzureBlob {
  implicit val fileSystemObject: FileSystemObject.Aux[AzureBlob, AccessTier] = new FileSystemObject[AzureBlob] {
    type StorageClassType = AccessTier

    override def name(a: AzureBlob): String = a.blob

    override def size(a: AzureBlob): Option[Long] =
      a.properties.flatMap(bp => Option(bp.getContentLength.longValue()))

    override def isDir(a: AzureBlob): Boolean = a.properties.isEmpty

    override def lastModified(a: AzureBlob): Option[Instant] =
      a.properties.flatMap(bp => Option(bp.getLastModified).map(_.toInstant))

    override def storageClass(a: AzureBlob): Option[StorageClassType] =
      a.properties.flatMap(bp => Option(bp.getAccessTier))

    override def universal(a: AzureBlob): UniversalFileSystemObject = UniversalFileSystemObject(
      name(a),
      size(a),
      isDir(a),
      storageClass(a).map {
        case AccessTier.ARCHIVE => GeneralStorageClass.ColdStorage
        case _                  => GeneralStorageClass.Standard
      },
      lastModified(a)
    )
  }
}
