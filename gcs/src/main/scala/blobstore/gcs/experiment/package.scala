package blobstore.gcs

import java.time.Instant

import blobstore.url.FsObject
import com.google.cloud.storage.{Blob => GcsBlob}

package object experiment {

  implicit val blob: FsObject[GcsBlob] = new FsObject[GcsBlob] {
    override def name(a: GcsBlob): String = a.getName

    override def size(a: GcsBlob): Long = a.getSize

    override def isDir(a: GcsBlob): Boolean = a.isDirectory

    override def lastModified(a: GcsBlob): Option[Instant] =
      Option(a.getUpdateTime: java.lang.Long).map(millis => Instant.ofEpochMilli(millis))
  }
}
