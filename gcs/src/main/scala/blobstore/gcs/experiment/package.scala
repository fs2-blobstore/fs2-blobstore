package blobstore.gcs

import blobstore.url.Blob
import com.google.cloud.storage.{Blob => GcsBlob}
import shapeless.Witness

package object experiment {

  type GcsProtocol = Witness.`"gs"`.T

  implicit val blob: Blob[GcsBlob] = new url.Blob[GcsBlob] {
    override def toUrl(a: GcsBlob): Url.PlainUrl = Url.standardUnsafe(show"gs://${a.getBucket}/${a.getName}")

    override def authority(a: GcsBlob): Authority = Bucket.unsafe(a.getBucket)

    override def size(a: GcsBlob): Long = a.getSize

    override def isDir(a: GcsBlob): Boolean = a.isDirectory

    override def lastModified(a: GcsBlob): Option[Instant] =
      Option(a.getUpdateTime: java.lang.Long).map(millis => Instant.ofEpochMilli(millis))
  }
}
