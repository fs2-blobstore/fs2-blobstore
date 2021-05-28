---
layout: docs
title: Data model
---

# Store

`Store` is the key abstraction of this library, it provides a single interface for various storage providers that lets you to read, write and delete data. It comes in two flavours depending on how the storage provider addresses objects.

* `blobstore.Store` is the common interface for all storage providers. It addresses objects using URLs. Examples of stores are `S3Store`, `GcsStore`, and `AzureStore`.
* `blobstore.PathStore` is the interface for hierarchical storage providers, typically with a fixed host. These stores are addressable using paths, examples are `SftpStore`, `FileStore` (for local filesystem), and `BoxStore`. Instances of `PathStore` can be lifted to a `Store`, URLs are then (optionally) validated against the fixed hostname of the `PathStore`.

The goal of the `Store` interface is to have a common representation of key/value functionality (get, put, list, etc) as streams that can be composed, transformed and piped just like any other `fs2.Stream` or `fs2.Pipe` regardless of the underlying storage mechanism

The three main activities in a key/value store are modeled like:

```scala
def list[A](path: Url[A]): fs2.Stream[F, Url[BlobType]]
def get[A](path: Url[A], chunkSize: Int): fs2.Stream[F, Byte]
def put(path: Url[A]): Pipe[F, Byte, Unit] 
```

You can reason parametrically about this API. Since there's no bounds on `A`, the implementation only uses information available in `Url`. More about this type parameter in the next section.

## Concrete stores

We provide pure functional `Store` implementations based on [fs2](https://fs2.io) and [cats-effect](https://typelevel.org/cats-effect/) for several storage technologies, for example `S3Store[F]`, `AzureStore[F]`, `GcsStore[F]` or `SftpStore[F]`. While `Store[F]` provide the generic API that allows you to abstract over many storage technologies, the storage specific implementations provide a much richer API, and should completely cover any use case you have for that specific provider. If you for instance want to use server side encryption, or any provider specific features, then the concrete stores is what you should use.

# `Url[A]` and `Path[A]`

Urls and paths are the key abstractions for addressing objects in stores. They are parameterized on the type of the object they point to, you can think of `Url[A]` and `Path[A]` as pointers to objects of type `A`. If there are no resolved object to point to, they are parameterized with `String`; `Url[String]` and `Path[String]` represents unresolved pointers, they are aliased to `Url.Plain` and `Path.Plain` respectively. You instantiate plain URLs either through our `String` validators, product type API, or the escape hatch `unsafe`:

```scala mdoc
import cats.ApplicativeThrow
import cats.data.ValidatedNec
import cats.effect.Concurrent

import blobstore.url.exception.UrlParseError
import blobstore.url.{Url, Authority, Path, Hostname}

def urlExample[F[_]: ApplicativeThrow] = {
  val u1: ValidatedNec[UrlParseError, Url.Plain] = Url.parse("s3://foo")
  val u2: F[Url.Plain] = Url.parseF[F]("s3://foo")
  val u3: Url.Plain = Url("s3", Authority.unsafe("foo"), Path("bar"))
  val u4: Url.Plain = Url.unsafe("s3://foo")
  
  ()
}
```


Once you can have a `Url.Plain`, you can use `list` or `stat` to get a pointer to a real object—if it exists. Here is an example program that transfers objects from GCS to S3, and filters out objects in cold storage: 

```scala mdoc
import blobstore.gcs.GcsStore
import blobstore.s3.S3Store

def transferExample[F[_]: Concurrent](
  gcs: GcsStore[F],
  gcsPrefix: Url.Plain, 
  s3: S3Store[F], 
  s3Bucket: Hostname): fs2.Stream[F, Unit] = {
    import com.google.cloud.storage.StorageClass

    gcs.list(gcsPrefix)                                                         // : Stream[F, Url[GcsBlob]]
      .filter(_.representation.storageClass.exists(_ == StorageClass.STANDARD)) // : Stream[F, Url[GcsBlob]]
      .map(url => gcs.get(url).through(s3.put(url.toS3(s3Bucket))))             // : Stream[F, Stream[F, Unit]]
      .parJoin(maxOpen = 200)                                                   // : Stream[F, Unit]
}
```

You can see in the example above that given a `Url[GcsBlob]` you can access the storage class of the underlying object, and the storage class is surfaced with the native GCS types. This gives you access to storage provider specific storage classes, such as S3's Glacier or Azure's Hot access tier.

You can also directly access the underlying storage provider's representation of the blob, if our Scala API doesn't have the information you need:

```scala mdoc:compile-only
import cats.effect.IO
import blobstore.gcs.GcsBlob
import com.google.cloud.storage.StorageOptions

val gcsStore: GcsStore[IO] = GcsStore(StorageOptions.getDefaultInstance.getService)

val stat: fs2.Stream[IO, Url[GcsBlob]] = gcsStore.stat(Url.unsafe("gs://foo/bar"))
stat.map { url =>
  val gcsBlob: blobstore.gcs.GcsBlob = url.path.representation
  val blobInfo: com.google.cloud.storage.BlobInfo = gcsBlob.blob

  gcsBlob -> blobInfo
}
```


Analogous APIs are available for all storage providers

In [stores](./stores) we look closer at how to abstract over storage providers, and in the storage technology specific pages, we show typical setups for each provider.

# A short note on URL terminology

This library uses the term "URL" in a wider sense than what the IETF standards strictly speaking allows for. We believe our URLs are more in line with how developers think, talk about, and use URLs. The discrepancy with IETF's standards (specifically [RFC3986](https://tools.ietf.org/html/rfc3986)) is so small that it should cause little concern.

For example, `s3://foo/bar.txt` will parse as a valid URL in our parser, this would not pass for instance the `java.net.URL` parser, nor is "s3" a valid URI scheme on IANA's [list of valid URI schemes](https://www.iana.org/assignments/uri-schemes/uri-schemes.xhtml).

We also support file URLs:

```scala mdoc
Url.unsafe("file:///home/foo/bar.txt")
```

File URLs will always have `localhost` as hostname. This makes it possible to treat `FileStore[F]`, our store implementation for the local filesystem, as any other `Store[F]`.

## Why not URIs

URIs have much less structure than URLs, and it would allow users to construct a lot more invalid input to our APIs. Our URL type endeavors to strike a good balance between type safety and ergonomics. Here's a couple of examples of valid URIs that would not pass our (or anyone else's) URL validators:

```scala mdoc
new java.net.URI("foo")
new java.net.URI("foo@example.com")
new java.net.URI("https//example.com")
new java.net.URI("https://example.com")
new java.net.URI("https:/example.com")
new java.net.URI("https::/example.com")
```

and so on, and so forth