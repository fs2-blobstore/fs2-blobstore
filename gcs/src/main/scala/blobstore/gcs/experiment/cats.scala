package blobstore.gcs.experiment

import _root_.cats.Show
import _root_.cats.syntax.all._
import _root_.cats.Order
import com.google.cloud.storage.{Blob, BlobId}

/**
  * Cats instances for GCS types
  */
object cats {

  implicit val showBlobId: Show[BlobId] = id => s"gs://${id.getBucket}/${id.getName}"
  implicit val showBlob: Show[Blob]     = id => showBlobId.show(id.getBlobId)

  implicit val orderBlobId: Order[BlobId] = (x, y) => x.show.compare(y.show)
  implicit val orderBlob: Order[Blob] = (x, y) => x.show.compare(y.show)
}
