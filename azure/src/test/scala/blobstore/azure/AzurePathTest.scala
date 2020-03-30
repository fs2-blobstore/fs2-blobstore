package blobstore
package azure

import java.time.ZoneOffset

import com.azure.storage.blob.models.BlobItemProperties

class AzurePathTest extends AbstractPathTest[AzurePath] {
  val properties = new BlobItemProperties()
    .setContentLength(fileSize)
    .setLastModified(lastModified.atOffset(ZoneOffset.UTC))
  override val concretePath: AzurePath = AzurePath(root, dir ++ "/" ++ fileName, Some(properties), Map.empty)
}
