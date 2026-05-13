package blobstore
package s3

import blobstore.url.Authority
import cats.syntax.all.*

object S3Authority {

  // <ap-name>-<account-id>.s3-accesspoint[.dualstack]?.<region>.amazonaws.com[.cn]?
  private val AccessPointVhost =
    """(?i)^([a-z0-9][a-z0-9-]*)-(\d{12})\.s3-accesspoint(?:\.dualstack)?\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?$""".r

  // <bucket>.s3[.-region|.dualstack.region|.region|.amazonaws.com|.amazonaws.com.cn]
  private val StandardVhost =
    """(?i)^([a-z0-9][a-z0-9.-]*?)\.s3(?:[-.][a-z0-9-]+)?\.amazonaws\.com(?:\.cn)?$""".r

  /** Resolves an Authority to the bucket identifier expected by the AWS S3 SDK.
    *
    * Virtual-hosted-style access-point hostnames are converted to the access-point ARN; virtual-hosted-style bucket
    * hostnames are reduced to the bucket name; everything else (plain bucket names, access-point aliases) passes
    * through unchanged. Routing an ARN across regions/partitions requires the caller's S3 client to be built with
    * `S3Configuration.useArnRegionEnabled(true)`.
    */
  def bucketParam(authority: Authority): String =
    authority.host.show match {
      case AccessPointVhost(apName, accountId, region) =>
        s"arn:${partitionFor(region)}:s3:$region:$accountId:accesspoint/$apName"
      case StandardVhost(bucket) => bucket
      case _                     => authority.show
    }

  private def partitionFor(region: String): String = {
    val r = region.toLowerCase
    if (r.startsWith("cn-")) "aws-cn"
    else if (r.startsWith("us-gov-")) "aws-us-gov"
    else "aws"
  }
}
