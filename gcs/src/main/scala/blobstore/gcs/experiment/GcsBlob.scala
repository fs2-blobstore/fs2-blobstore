package blobstore.gcs.experiment

import java.time.Instant

import _root_.cats.instances.string._
import _root_.cats.syntax.all._
import _root_.cats.{Eq, Order, Show}
import blobstore.experiment.url
import blobstore.experiment.url.{Authority, Url}
import blobstore.experiment.url.Authority.Bucket
import blobstore.gcs.experiment.cats._
import com.google.cloud.storage.Blob

case class GcsBlob(blob: Blob)

object GcsBlob {
  implicit val blob: url.Blob[Blob] = new url.Blob[Blob] {
    override def toUrl(a: Blob): Url.PlainUrl = Url.standardUnsafe(show"gs://${a.getBucket}/${a.getName}")

    override def authority(a: Blob): Authority = Bucket.unsafe(a.getBucket)

    override def size(a: Blob): Long = a.getSize

    override def isDir(a: Blob): Boolean = a.isDirectory

    override def lastModified(a: Blob): Option[Instant] =
      Option(a.getUpdateTime: java.lang.Long).map(millis => Instant.ofEpochMilli(millis))
  }

  implicit val show: Show[GcsBlob]   = Show[Blob].contramap(_.blob)
  implicit val eq: Eq[GcsBlob]       = (x, y) => Eq[Blob].eqv(x.blob, y.blob)
  implicit val order: Order[GcsBlob] = (x: GcsBlob, y: GcsBlob) => Order[Blob].compare(x.blob, y.blob)
}
