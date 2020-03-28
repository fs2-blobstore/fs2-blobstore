package blobstore.gcs

import java.math.BigInteger

import blobstore.AbstractPathTest
import com.google.api.client.util.DateTime
import com.google.api.services.storage.model.StorageObject
import com.google.cloud.storage.BlobInfo

class GcsPathTest extends AbstractPathTest[GcsPath] {
  @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
  val blobInfo: BlobInfo = {
    val storageObject = new StorageObject()
    storageObject.setBucket(root)
    storageObject.setName(dir ++ "/" ++ fileName)
    storageObject.setSize(new BigInteger(fileSize.toString))
    storageObject.setUpdated(new DateTime(lastModified.toEpochMilli))
    val fromPb = classOf[BlobInfo].getDeclaredMethod("fromPb", classOf[StorageObject])
    fromPb.setAccessible(true)
    fromPb.invoke(classOf[BlobInfo], storageObject).asInstanceOf[BlobInfo]
  }

  val concretePath = new GcsPath(blobInfo)
}
