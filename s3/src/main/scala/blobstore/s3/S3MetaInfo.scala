package blobstore.s3

import java.time.Instant

import software.amazon.awssdk.services.s3.model.*

import scala.jdk.CollectionConverters.*

trait S3MetaInfo {
  def tags: Option[Tagging]                                        = None
  def size: Option[Long]                                           = None
  def acl: Option[ObjectCannedACL]                                 = None
  def eTag: Option[String]                                         = None
  def lastModified: Option[Instant]                                = None
  def storageClass: Option[StorageClass]                           = None
  def deleteMarker: Boolean                                        = false
  def range: Option[String]                                        = None
  def acceptRanges: Option[String]                                 = None
  def expiration: Option[String]                                   = None
  def restore: Option[String]                                      = None
  def missingMeta: Option[Int]                                     = None
  def versionId: Option[String]                                    = None
  def cacheControl: Option[String]                                 = None
  def contentDisposition: Option[String]                           = None
  def contentEncoding: Option[String]                              = None
  def contentLanguage: Option[String]                              = None
  def contentType: Option[String]                                  = None
  def contentMD5: Option[String]                                   = None
  def expires: Option[Instant]                                     = None
  def websiteRedirectLocation: Option[String]                      = None
  def serverSideEncryption: Option[ServerSideEncryption]           = None
  def metadata: Map[String, String]                                = Map.empty
  def sseCustomerAlgorithm: Option[String]                         = None
  def sseCustomerKey: Option[String]                               = None
  def sseCustomerKeyMD5: Option[String]                            = None
  def sseKmsKeyId: Option[String]                                  = None
  def sseKmsEncryptionContext: Option[String]                      = None
  def requestCharged: Option[RequestCharged]                       = None
  def requestPayer: Option[RequestPayer]                           = None
  def replicationStatus: Option[ReplicationStatus]                 = None
  def partsCount: Option[Int]                                      = None
  def objectLockMode: Option[ObjectLockMode]                       = None
  def objectLockRetainUntilDate: Option[Instant]                   = None
  def objectLockLegalHoldStatus: Option[ObjectLockLegalHoldStatus] = None

  override def toString: String =
    List(
      tags.flatMap {
        case tg if tg.hasTagSet =>
          Some("Tags: " ++ tg.tagSet().asScala.map(t => List(t.key(), t.value()).mkString(":")).mkString("[", ",", "]"))
        case _ => None
      },
      size.map("Size: " ++ _.toString),
      acl.map("ACL: " ++ _.toString),
      eTag.map("ETag: " ++ _),
      lastModified.map("Last Modified: " ++ _.toString),
      storageClass.map("Storage Class: " ++ _.toString),
      if (deleteMarker) Some("Delete Marker: true") else None,
      range.map("Range: " ++ _),
      acceptRanges.map("Accept Ranges: " ++ _),
      expiration.map("Expiration: " ++ _),
      restore.map("Restore: " ++ _),
      missingMeta.flatMap {
        case 0 => None
        case n => Some(s"Missing Meta: $n")
      },
      versionId.map("Version Id: " ++ _),
      cacheControl.map("Cache Control: " ++ _),
      contentDisposition.map("Content Disposition: " ++ _),
      contentEncoding.map("Content Encoding: " ++ _),
      contentLanguage.map("Content Language: " ++ _),
      contentType.map("Content Type: " ++ _),
      contentMD5.map("Content MD5: " ++ _),
      expires.map("Expires: " ++ _.toString),
      websiteRedirectLocation.map("Website Redirect Location: " ++ _),
      serverSideEncryption.map("Server Side Encryption: " ++ _.toString),
      if (metadata.isEmpty) None else Some("Metadata: " ++ metadata.mkString("{", ",", "}")),
      sseCustomerAlgorithm.map("SSE Customer Algorithm: " ++ _),
      sseCustomerKey.map("SSE Customer Key: " ++ _),
      sseCustomerKeyMD5.map("SSE Customer Key MD5: " ++ _),
      sseKmsKeyId.map("SSE Kms Key Id: " ++ _),
      sseKmsEncryptionContext.map("SSE Kms Encryption Context: " ++ _),
      requestCharged.map("Request Charged: " ++ _.toString),
      requestPayer.map("Request Payer: " ++ _.toString),
      replicationStatus.map("Replication Status: " ++ _.toString),
      partsCount.map("Parts Count: " ++ _.toString),
      objectLockMode.map("Object Lock Mode: " ++ _.toString),
      objectLockRetainUntilDate.map("Object Lock Retain Until Date: " ++ _.toString),
      objectLockLegalHoldStatus.map("Object Lock Legal Hold Status: " ++ _.toString)
    ).flatten.sorted.mkString("S3MetaInfo(", ", ", ")")
}

object S3MetaInfo {
  class S3ObjectMetaInfo(s3Object: S3Object) extends S3MetaInfo {
    override def size: Option[Long]            = Option(s3Object.size(): Long)
    override def lastModified: Option[Instant] = Option(s3Object.lastModified())
    override def storageClass: Option[StorageClass] =
      Option(s3Object.storageClass()).map(osc => StorageClass.fromValue(osc.toString))
    override def eTag: Option[String] = Option(s3Object.eTag())
  }

  class HeadObjectResponseMetaInfo(headObjectResponse: HeadObjectResponse) extends S3MetaInfo {
    override def size: Option[Long]                      = Option(headObjectResponse.contentLength(): Long)
    override def lastModified: Option[Instant]           = Option(headObjectResponse.lastModified())
    override def eTag: Option[String]                    = Option(headObjectResponse.eTag())
    override def storageClass: Option[StorageClass]      = Option(headObjectResponse.storageClass())
    override def deleteMarker: Boolean                   = Option(headObjectResponse.deleteMarker(): Boolean).getOrElse[Boolean](false)
    override def acceptRanges: Option[String]            = Option(headObjectResponse.acceptRanges())
    override def expiration: Option[String]              = Option(headObjectResponse.expiration())
    override def restore: Option[String]                 = Option(headObjectResponse.restore())
    override def missingMeta: Option[Int]                = Option(headObjectResponse.missingMeta(): Int)
    override def versionId: Option[String]               = Option(headObjectResponse.versionId())
    override def cacheControl: Option[String]            = Option(headObjectResponse.cacheControl())
    override def contentDisposition: Option[String]      = Option(headObjectResponse.contentDisposition())
    override def contentEncoding: Option[String]         = Option(headObjectResponse.contentEncoding())
    override def contentLanguage: Option[String]         = Option(headObjectResponse.contentLanguage())
    override def contentType: Option[String]             = Option(headObjectResponse.contentType())
    override def expires: Option[Instant]                = Option(headObjectResponse.expires())
    override def websiteRedirectLocation: Option[String] = Option(headObjectResponse.websiteRedirectLocation())
    override def serverSideEncryption: Option[ServerSideEncryption] =
      Option(headObjectResponse.serverSideEncryption())
    override def metadata: Map[String, String] =
      Option(headObjectResponse.metadata().asScala.toMap).getOrElse(Map.empty)
    override def sseCustomerAlgorithm: Option[String]         = Option(headObjectResponse.sseCustomerAlgorithm())
    override def sseCustomerKeyMD5: Option[String]            = Option(headObjectResponse.sseCustomerKeyMD5())
    override def sseKmsKeyId: Option[String]                  = Option(headObjectResponse.ssekmsKeyId())
    override def requestCharged: Option[RequestCharged]       = Option(headObjectResponse.requestCharged())
    override def replicationStatus: Option[ReplicationStatus] = Option(headObjectResponse.replicationStatus())
    override def partsCount: Option[Int]                      = Option(headObjectResponse.partsCount(): Int)
    override def objectLockMode: Option[ObjectLockMode]       = Option(headObjectResponse.objectLockMode())
    override def objectLockRetainUntilDate: Option[Instant] =
      Option(headObjectResponse.objectLockRetainUntilDate())
    override def objectLockLegalHoldStatus: Option[ObjectLockLegalHoldStatus] =
      Option(headObjectResponse.objectLockLegalHoldStatus())
  }

  def const(
    constTags: Option[Tagging] = None,
    constSize: Option[Long] = None,
    constETag: Option[String] = None,
    constLastModified: Option[Instant] = None,
    constStorageClass: Option[StorageClass] = None,
    constDeleteMarker: Boolean = false,
    constRange: Option[String] = None,
    constAcceptRanges: Option[String] = None,
    constExpiration: Option[String] = None,
    constRestore: Option[String] = None,
    constMissingMeta: Option[Int] = None,
    constVersionId: Option[String] = None,
    constCacheControl: Option[String] = None,
    constContentDisposition: Option[String] = None,
    constContentEncoding: Option[String] = None,
    constContentLanguage: Option[String] = None,
    constContentType: Option[String] = None,
    constExpires: Option[Instant] = None,
    constWebsiteRedirectLocation: Option[String] = None,
    constServerSideEncryption: Option[ServerSideEncryption] = None,
    constMetadata: Map[String, String] = Map.empty,
    constSseCustomerAlgorithm: Option[String] = None,
    constSseCustomerKey: Option[String] = None,
    constSseCustomerKeyMD5: Option[String] = None,
    constSseKmsKeyId: Option[String] = None,
    constSseKmsEncryptionContext: Option[String] = None,
    constRequestCharged: Option[RequestCharged] = None,
    constRequestPayer: Option[RequestPayer] = None,
    constReplicationStatus: Option[ReplicationStatus] = None,
    constPartsCount: Option[Int] = None,
    constObjectLockMode: Option[ObjectLockMode] = None,
    constObjectLockRetainUntilDate: Option[Instant] = None,
    constObjectLockLegalHoldStatus: Option[ObjectLockLegalHoldStatus] = None,
    constAcl: Option[ObjectCannedACL] = None,
    constContentMd5: Option[String] = None
  ): S3MetaInfo = new S3MetaInfo {
    override val tags: Option[Tagging]                                        = constTags
    override val size: Option[Long]                                           = constSize
    override val eTag: Option[String]                                         = constETag
    override val lastModified: Option[Instant]                                = constLastModified
    override val storageClass: Option[StorageClass]                           = constStorageClass
    override val deleteMarker: Boolean                                        = constDeleteMarker
    override val range: Option[String]                                        = constRange
    override val acceptRanges: Option[String]                                 = constAcceptRanges
    override val expiration: Option[String]                                   = constExpiration
    override val restore: Option[String]                                      = constRestore
    override val missingMeta: Option[Int]                                     = constMissingMeta
    override val versionId: Option[String]                                    = constVersionId
    override val cacheControl: Option[String]                                 = constCacheControl
    override val contentDisposition: Option[String]                           = constContentDisposition
    override val contentEncoding: Option[String]                              = constContentEncoding
    override val contentLanguage: Option[String]                              = constContentLanguage
    override val contentType: Option[String]                                  = constContentType
    override val expires: Option[Instant]                                     = constExpires
    override val websiteRedirectLocation: Option[String]                      = constWebsiteRedirectLocation
    override val serverSideEncryption: Option[ServerSideEncryption]           = constServerSideEncryption
    override val metadata: Map[String, String]                                = constMetadata
    override val sseCustomerAlgorithm: Option[String]                         = constSseCustomerAlgorithm
    override val sseCustomerKeyMD5: Option[String]                            = constSseCustomerKeyMD5
    override val sseKmsKeyId: Option[String]                                  = constSseKmsKeyId
    override val sseKmsEncryptionContext: Option[String]                      = constSseKmsEncryptionContext
    override val requestCharged: Option[RequestCharged]                       = constRequestCharged
    override val requestPayer: Option[RequestPayer]                           = constRequestPayer
    override val replicationStatus: Option[ReplicationStatus]                 = constReplicationStatus
    override val partsCount: Option[Int]                                      = constPartsCount
    override val objectLockMode: Option[ObjectLockMode]                       = constObjectLockMode
    override val objectLockRetainUntilDate: Option[Instant]                   = constObjectLockRetainUntilDate
    override val objectLockLegalHoldStatus: Option[ObjectLockLegalHoldStatus] = constObjectLockLegalHoldStatus
    override val acl: Option[ObjectCannedACL]                                 = constAcl
    override val contentMD5: Option[String]                                   = constContentMd5
    override val sseCustomerKey: Option[String]                               = constSseCustomerKey
  }

  def mkPutObjectRequest(
    sseAlgorithm: Option[String],
    objectAcl: Option[ObjectCannedACL],
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    size: Long
  ): PutObjectRequest = {
    val builder = PutObjectRequest.builder()
    val withAcl = objectAcl.fold(builder)(builder.acl)
    val withSSE = sseAlgorithm.fold(withAcl)(withAcl.serverSideEncryption)

    def addMeta(b: PutObjectRequest.Builder)(metaInfo: S3MetaInfo): PutObjectRequest.Builder = {
      var builder = b // scalafix:ok
      builder = metaInfo.acl.fold(builder)(builder.acl)
      builder = metaInfo.cacheControl.fold(builder)(builder.cacheControl)
      builder = metaInfo.contentDisposition.fold(builder)(builder.contentDisposition)
      builder = metaInfo.contentEncoding.fold(builder)(builder.contentEncoding)
      builder = metaInfo.contentLanguage.fold(builder)(builder.contentLanguage)
      builder = metaInfo.contentMD5.fold(builder)(builder.contentMD5)
      builder = metaInfo.contentType.fold(builder)(builder.contentType)
      builder = metaInfo.expires.fold(builder)(builder.expires)
      builder = if (metaInfo.metadata.isEmpty) builder else builder.metadata(metaInfo.metadata.asJava)
      builder = metaInfo.serverSideEncryption.fold(builder)(builder.serverSideEncryption)
      builder = metaInfo.storageClass.fold(builder)(builder.storageClass)
      builder = metaInfo.websiteRedirectLocation.fold(builder)(builder.websiteRedirectLocation)
      builder = metaInfo.sseCustomerAlgorithm.fold(builder)(builder.sseCustomerAlgorithm)
      builder = metaInfo.sseCustomerKey.fold(builder)(builder.sseCustomerKey)
      builder = metaInfo.sseCustomerKeyMD5.fold(builder)(builder.sseCustomerKeyMD5)
      builder = metaInfo.sseKmsKeyId.fold(builder)(builder.ssekmsKeyId)
      builder = metaInfo.sseKmsEncryptionContext.fold(builder)(builder.ssekmsEncryptionContext)
      builder = metaInfo.requestPayer.fold(builder)(builder.requestPayer)
      builder = metaInfo.tags.fold(builder)(builder.tagging)
      builder = metaInfo.objectLockMode.fold(builder)(builder.objectLockMode)
      builder = metaInfo.objectLockRetainUntilDate.fold(builder)(builder.objectLockRetainUntilDate)
      metaInfo.objectLockLegalHoldStatus.fold(builder)(builder.objectLockLegalHoldStatus)
    }

    meta
      .fold(withSSE)(addMeta(withSSE))
      .contentLength(size)
      .bucket(bucket)
      .key(key)
      .build()
  }

  def mkPutMultiPartRequest(
    sseAlgorithm: Option[String],
    objectAcl: Option[ObjectCannedACL],
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo]
  ): CreateMultipartUploadRequest = {
    val builder = CreateMultipartUploadRequest.builder()
    val withAcl = objectAcl.fold(builder)(builder.acl)
    val withSSE = sseAlgorithm.fold(withAcl)(withAcl.serverSideEncryption)

    def addMeta(b: CreateMultipartUploadRequest.Builder)(metaInfo: S3MetaInfo): CreateMultipartUploadRequest.Builder = {
      var builder = b // scalafix:ok
      builder = metaInfo.acl.fold(builder)(builder.acl)
      builder = metaInfo.cacheControl.fold(builder)(builder.cacheControl)
      builder = metaInfo.contentDisposition.fold(builder)(builder.contentDisposition)
      builder = metaInfo.contentEncoding.fold(builder)(builder.contentEncoding)
      builder = metaInfo.contentLanguage.fold(builder)(builder.contentLanguage)
      builder = metaInfo.contentType.fold(builder)(builder.contentType)
      builder = metaInfo.expires.fold(builder)(builder.expires)
      builder = if (metaInfo.metadata.isEmpty) builder else builder.metadata(metaInfo.metadata.asJava)
      builder = metaInfo.serverSideEncryption.fold(builder)(builder.serverSideEncryption)
      builder = metaInfo.storageClass.fold(builder)(builder.storageClass)
      builder = metaInfo.websiteRedirectLocation.fold(builder)(builder.websiteRedirectLocation)
      builder = metaInfo.sseCustomerAlgorithm.fold(builder)(builder.sseCustomerAlgorithm)
      builder = metaInfo.sseCustomerKey.fold(builder)(builder.sseCustomerKey)
      builder = metaInfo.sseCustomerKeyMD5.fold(builder)(builder.sseCustomerKeyMD5)
      builder = metaInfo.sseKmsKeyId.fold(builder)(builder.ssekmsKeyId)
      builder = metaInfo.sseKmsEncryptionContext.fold(builder)(builder.ssekmsEncryptionContext)
      builder = metaInfo.requestPayer.fold(builder)(builder.requestPayer)
      builder = metaInfo.tags.fold(builder)(builder.tagging)
      builder = metaInfo.objectLockMode.fold(builder)(builder.objectLockMode)
      builder = metaInfo.objectLockRetainUntilDate.fold(builder)(builder.objectLockRetainUntilDate)
      metaInfo.objectLockLegalHoldStatus.fold(builder)(builder.objectLockLegalHoldStatus)
    }

    meta
      .fold(withSSE)(addMeta(withSSE))
      .bucket(bucket)
      .key(key)
      .build()
  }

  def mkCopyObjectRequest(
    sseAlgorithm: Option[String],
    objectAcl: Option[ObjectCannedACL],
    source: String,
    dstBucket: String,
    dstKey: String,
    meta: Option[S3MetaInfo]
  ): CopyObjectRequest = {
    val builder = CopyObjectRequest.builder()
    val withAcl = objectAcl.fold(builder)(builder.acl)
    val withSSE = sseAlgorithm.fold(withAcl)(withAcl.serverSideEncryption)

    def addMeta(b: CopyObjectRequest.Builder)(metaInfo: S3MetaInfo): CopyObjectRequest.Builder = {
      var builder = b // scalafix:ok
      builder = metaInfo.acl.fold(builder)(builder.acl)
      builder = metaInfo.cacheControl.fold(builder)(builder.cacheControl)
      builder = metaInfo.contentDisposition.fold(builder)(builder.contentDisposition)
      builder = metaInfo.contentEncoding.fold(builder)(builder.contentEncoding)
      builder = metaInfo.contentLanguage.fold(builder)(builder.contentLanguage)
      builder = metaInfo.contentType.fold(builder)(builder.contentType)
      builder = metaInfo.expires.fold(builder)(builder.expires)
      builder = if (metaInfo.metadata.isEmpty) builder else builder.metadata(metaInfo.metadata.asJava)
      builder = metaInfo.serverSideEncryption.fold(builder)(builder.serverSideEncryption)
      builder = metaInfo.storageClass.fold(builder)(builder.storageClass)
      builder = metaInfo.websiteRedirectLocation.fold(builder)(builder.websiteRedirectLocation)
      builder = metaInfo.sseCustomerAlgorithm.fold(builder)(builder.sseCustomerAlgorithm)
      builder = metaInfo.sseCustomerKey.fold(builder)(builder.sseCustomerKey)
      builder = metaInfo.sseCustomerKeyMD5.fold(builder)(builder.sseCustomerKeyMD5)
      builder = metaInfo.sseKmsKeyId.fold(builder)(builder.ssekmsKeyId)
      builder = metaInfo.sseKmsEncryptionContext.fold(builder)(builder.ssekmsEncryptionContext)
      builder = metaInfo.requestPayer.fold(builder)(builder.requestPayer)
      builder = metaInfo.tags.fold(builder)(builder.tagging)
      builder = metaInfo.objectLockMode.fold(builder)(builder.objectLockMode)
      builder = metaInfo.objectLockRetainUntilDate.fold(builder)(builder.objectLockRetainUntilDate)
      metaInfo.objectLockLegalHoldStatus.fold(builder)(builder.objectLockLegalHoldStatus)
    }

    meta
      .fold(withSSE)(addMeta(withSSE))
      .copySource(source)
      .destinationBucket(dstBucket)
      .destinationKey(dstKey)
      .build()
  }

  def mkGetObjectRequest(bucket: String, key: String, metaInfo: S3MetaInfo): GetObjectRequest = {
    var builder = GetObjectRequest.builder() // scalafix:ok
    builder = metaInfo.range.fold(builder)(builder.range)
    builder = metaInfo.versionId.fold(builder)(builder.versionId)
    builder = metaInfo.requestPayer.fold(builder)(builder.requestPayer)
    builder = metaInfo.sseCustomerAlgorithm.fold(builder)(builder.sseCustomerAlgorithm)
    builder = metaInfo.sseCustomerKey.fold(builder)(builder.sseCustomerKey)
    builder = metaInfo.sseCustomerKeyMD5.fold(builder)(builder.sseCustomerKeyMD5)
    builder.bucket(bucket).key(key).build()
  }

  def mkUploadPartRequestBuilder(
    bucket: String,
    key: String,
    uploadId: String,
    meta: Option[S3MetaInfo],
    part: Int,
    length: Option[Long]
  ): UploadPartRequest = {
    val builder = UploadPartRequest.builder()

    def addMeta(b: UploadPartRequest.Builder)(metaInfo: S3MetaInfo): UploadPartRequest.Builder = {
      var builder = b // scalafix:ok
      builder = metaInfo.sseCustomerAlgorithm.fold(builder)(builder.sseCustomerAlgorithm)
      builder = metaInfo.sseCustomerKey.fold(builder)(builder.sseCustomerKey)
      metaInfo.sseCustomerKeyMD5.fold(builder)(builder.sseCustomerKeyMD5)
    }

    val b = meta.fold(builder)(addMeta(builder)).bucket(bucket).key(key).uploadId(uploadId).partNumber(part)

    length.fold(b)(l => b.contentLength(l)).build()
  }
}
