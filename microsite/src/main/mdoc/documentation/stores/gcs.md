---
layout: docs
title: GCS
---

# GCS

This store is used to interface with Google's Cloud Storage. Head over to the official [documentation](https://cloud.google.com/storage/docs/reference/libraries#client-libraries-usage-java) for the Java client for details on how to configure it. Here is a vanilla setup that uses credentials from your environment:

```scala mdoc:silent
import blobstore.gcs.GcsStore
import blobstore.url.Url

import cats.effect.IO
import squants.information.Mebibytes
import com.google.cloud.storage.{Storage, StorageOptions}
import com.google.cloud.storage.Storage.BlobGetOption

val storage: Storage = StorageOptions.getDefaultInstance.getService
val gcs: GcsStore[IO] = GcsStore[IO](storage)
```

The generic API available in `Store[F]` does not let you use any GCS specific features. In order to do so, we provide dedicated methods that let's you pass GCS specific options, here's an example that uses GCS server side encryption:

```scala mdoc
 def gcsGet[F[_]](gcs: GcsStore[F], url: Url.Plain, decryptionKey: String): fs2.Stream[F, Byte] =
   gcs.get(url, Mebibytes(2).toBytes.toInt, List(BlobGetOption.decryptionKey(decryptionKey)))
```

Here we use [squants](https://github.com/typelevel/squants) to work with quantities of bytes. The API accept any `Int` and how you produce this `Int` is up to you.