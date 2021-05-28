---
layout: docs
title: Stores
---

Stores are the abstraction that encapsulate CRUD operations for any storage provider. The _generic api_, `Store[F]`, is a generic API that can be used across storage providers. Storage provider specific APIs for accessing features unique to that provider is provided in _specific stores_, for example `S3Store[F]`. 

# Accessing data in stores

Using vendor specific stores, for instance `GcsStore[F]`, gives you a rich API for accessing data in that store, as well as the ability to use the underlying vendor client's API if you need to:

```scala mdoc:silent
import blobstore.gcs.{GcsStore, GcsBlob}
import blobstore.url.Url

import cats.effect.IO
import com.google.cloud.storage.StorageOptions

val gcs = GcsStore[IO](StorageOptions.getDefaultInstance.getService)

gcs.list(Url.unsafe("gs://foo")).map { url: Url[GcsBlob] =>
  val repr: blobstore.gcs.GcsBlob = url.representation
  val gcsBlob: com.google.cloud.storage.BlobInfo = url.representation.blob
  val size = repr.size
  val size2 = gcsBlob.getSize
  
  size -> size2
}
```

But what if you want to express your logic independently of the underlying storage technology, and use different stores based on runtime input? This is what the `Store.Generic[F]` type is for. It exposes URLs of type `Url[FsObject]` where `FsObject` is the (semantically) least upper bound of any store object. This type contains a name and optional values for everything that _might_ exist.

We provide a special `GenericStorageClass` type that serves as a least upper bound for all storage classes. It's a co-product with only two members:

```scala mdoc
sealed trait GeneralStorageClass
object GeneralStorageClass {
  case object Standard    extends GeneralStorageClass
  case object ColdStorage extends GeneralStorageClass
}
```

`FsObject`s (optionally) expose this type as their storage class. If you need vendor specifc storage classes, the you need to use the vendor specific stores.

Here is an example of a service that decides what stores to use as source and sink at runtime.

```scala mdoc
import blobstore.s3.S3Store
import blobstore.gcs.GcsStore
import blobstore.url.FsObject
import blobstore.Store

import cats.syntax.all._

class TransferIt(gcs: GcsStore[IO], s3: S3Store[IO]) {
  def resolve(url: Url.Plain): IO[Store.Generic[IO]] =
    url.scheme match {
      case "s3"  => s3.pure[IO].widen
      case "gcs" => gcs.pure[IO].widen
      case _     => new Exception("Unknown scheme").raiseError[IO, Store.Generic[IO]]
    }

  def copy(from: Url.Plain, to: Url.Plain): IO[Unit] =
    (resolve(from), resolve(to)).tupled.flatMap {
      case (src: Store.Generic[IO], dst: Store.Generic[IO]) =>
        src.list(from, recursive = true)
          .filter((url: Url[FsObject]) => url.representation.storageClass.contains(GeneralStorageClass.Standard))
          .mapFilter((url: Url[FsObject]) => url.path.fileName.map(_ -> url))
          .map {
            case (fileName: String, obj: Url[FsObject]) => src.get(obj).through(dst.put(to / fileName))
          }
          .parJoin(maxOpen = 100)
          .compile
          .drain
    }
}
```

Once the compiler is asked to calculate the least upper bound of two vendor stores, for instance `GcsStore[IO]` and `S3Store[IO]` in the example above, it will correctly calculate the types for `Store.Generic[IO]`, `FsObject` and `GenericStorageClass`, as well as any transformations from the backing vendor specific types. 

We use the `.fileName` accessor on the URL path in the example above. By default, paths with a `/` suffix is interpreted as not having a file name, for example `/foo/bar/`. You can disable this behavior when constructing stores that supports objects with `/` suffixes, such as S3, GCS and Azure.

## PathStore

Path stores, such as `SftpStore[F]`, provides the same interface as `Store[F]`, but it uses `Path[A]` to address objects in the store instead of `Url[A]`. The `PathStore[A]` interface is otherwise largely identical to `Store[A]`:

```scala
  def list[A](path: Path[A], recursive: Boolean): fs2.Stream[F, Path[BlobType]]
  def get[A](path: Path[A], chunkSize: Int): fs2.Stream[F, Byte]
  def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit]
```

Just as with `Store[A]`, parametric reasoning tells you that since we know nothing about `A`, then these methods are implemented without any assumptions about the underlying object. You'll find optimized versions that does use knowledge about the underlying type in the technology specific stores, such as `SftpStore[F]` or `BoxStore[F]`.

## Abstracting over path stores

You can lift a `PathStore[F]` to a `Store[F]` by calling `.lift`. This gives you a store that operates with URLs; it will by default perform hostname validation against any fixed hostname in the underlying `PathStore[F]`.


Here is the `resolve` method from earlier extended to support SFTP servers:

```scala mdoc:nest
import blobstore.sftp.SftpStore

class TransferIt(gcs: GcsStore[IO], s3: S3Store[IO], sftp: SftpStore[IO]) {
  def resolve[A](url: Url.Plain): IO[Store.Generic[IO]] =
    url.scheme match {
      case "s3"   => s3.pure[IO].widen
      case "gcs"  => gcs.pure[IO].widen
      case "sftp" => sftp.lift.pure[IO].widen
      case _      => new Exception("Unknown scheme").raiseError[IO, Store.Generic[IO]]
    }
}
```