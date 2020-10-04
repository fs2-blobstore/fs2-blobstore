package blobstore.url.general

sealed trait GeneralStorageClass
object GeneralStorageClass {
  case object Standard    extends GeneralStorageClass
  case object ColdStorage extends GeneralStorageClass
}
