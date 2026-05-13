package blobstore.s3

import blobstore.url.Authority
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class S3AuthorityTest extends AnyFlatSpec with Matchers {

  behavior of "S3Authority.bucketParam"

  it should "reconstruct the access-point ARN from a regional vhost" in {
    S3Authority.bucketParam(Authority.unsafe("my-ap-123456789012.s3-accesspoint.us-west-2.amazonaws.com")) mustBe
      "arn:aws:s3:us-west-2:123456789012:accesspoint/my-ap"
  }

  it should "reconstruct the access-point ARN from a dualstack vhost" in {
    S3Authority.bucketParam(
      Authority.unsafe("my-ap-123456789012.s3-accesspoint.dualstack.eu-west-1.amazonaws.com")
    ) mustBe
      "arn:aws:s3:eu-west-1:123456789012:accesspoint/my-ap"
  }

  it should "use the aws-cn partition for China access-point vhosts" in {
    S3Authority.bucketParam(Authority.unsafe("my-ap-123456789012.s3-accesspoint.cn-north-1.amazonaws.com.cn")) mustBe
      "arn:aws-cn:s3:cn-north-1:123456789012:accesspoint/my-ap"
  }

  it should "use the aws-us-gov partition for GovCloud access-point vhosts" in {
    S3Authority.bucketParam(Authority.unsafe("my-ap-123456789012.s3-accesspoint.us-gov-west-1.amazonaws.com")) mustBe
      "arn:aws-us-gov:s3:us-gov-west-1:123456789012:accesspoint/my-ap"
  }

  it should "preserve hyphens inside the access-point name" in {
    S3Authority.bucketParam(
      Authority.unsafe("name-with-many-parts-123456789012.s3-accesspoint.us-east-1.amazonaws.com")
    ) mustBe
      "arn:aws:s3:us-east-1:123456789012:accesspoint/name-with-many-parts"
  }

  it should "extract the bucket name from a regional vhost" in {
    S3Authority.bucketParam(Authority.unsafe("my-bucket.s3.us-west-2.amazonaws.com")) mustBe "my-bucket"
  }

  it should "extract the bucket name from a legacy region-less vhost" in {
    S3Authority.bucketParam(Authority.unsafe("my-bucket.s3.amazonaws.com")) mustBe "my-bucket"
  }

  it should "extract the bucket name from the legacy dash-region vhost" in {
    S3Authority.bucketParam(Authority.unsafe("my-bucket.s3-us-west-2.amazonaws.com")) mustBe "my-bucket"
  }

  it should "extract the bucket name from a China vhost" in {
    S3Authority.bucketParam(Authority.unsafe("my-bucket.s3.cn-north-1.amazonaws.com.cn")) mustBe "my-bucket"
  }

  it should "pass plain bucket names through unchanged" in {
    S3Authority.bucketParam(Authority.unsafe("my-bucket")) mustBe "my-bucket"
  }

  it should "pass access-point aliases through unchanged" in {
    val alias = "my-ap-1a2b3c4d5e6f1234567890123456abcdef-s3alias"
    S3Authority.bucketParam(Authority.unsafe(alias)) mustBe alias
  }
}
