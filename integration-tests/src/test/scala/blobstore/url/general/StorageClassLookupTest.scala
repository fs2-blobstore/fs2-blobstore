package blobstore.url.general

import com.google.cloud.storage.{BlobId, BlobInfo}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import blobstore.gcs.GcsBlob
import blobstore.s3.{S3Blob, S3MetaInfo}
import blobstore.url.FsObject

class StorageClassLookupTest extends AnyFlatSpec with Matchers {

  behavior of "StorageClassLookup"

  it should "infer general storage class as least upper bound" in {

    val gcs: GcsBlob = GcsBlob(BlobInfo.newBuilder(BlobId.of("foo", "bar"))
      .setStorageClass(com.google.cloud.storage.StorageClass.NEARLINE).build())
    val s3: S3Blob = S3Blob(
      bucket = "foo",
      key = "bar",
      meta = Some(S3MetaInfo.const(constStorageClass =
        Some(software.amazon.awssdk.services.s3.model.StorageClass.DEEP_ARCHIVE)))
    )
    val lub1: FsObject = if (true) gcs else s3
    val lub2: FsObject = if (true) s3 else gcs

    val gcsStorageClass: Option[com.google.cloud.storage.StorageClass]                = gcs.storageClass
    val s3StorageClass: Option[software.amazon.awssdk.services.s3.model.StorageClass] = s3.storageClass
    val lubStorageClass: Option[GeneralStorageClass]                                  = lub1.storageClass
    val lub2StorageClass: Option[GeneralStorageClass]                                 = lub2.storageClass

    gcsStorageClass mustBe Some(com.google.cloud.storage.StorageClass.NEARLINE)
    s3StorageClass mustBe Some(software.amazon.awssdk.services.s3.model.StorageClass.DEEP_ARCHIVE)

    lubStorageClass mustBe Some(GeneralStorageClass.Standard)
    lub2StorageClass mustBe Some(GeneralStorageClass.ColdStorage)
  }

}
