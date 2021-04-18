package blobstore.url.general

trait StorageClassLookup[-A] {
  type StorageClassType

  def storageClass(a: A): Option[StorageClassType]
}

object StorageClassLookup {
  type Aux[-A, R] = StorageClassLookup[A] { type StorageClassType = R }

  def apply[A: StorageClassLookup]: StorageClassLookup[A] = implicitly[StorageClassLookup[A]]
}
