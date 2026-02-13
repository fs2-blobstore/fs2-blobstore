---
layout: docs
title: Box
---

# Box

This store is used to interface with Box. Head over to the official [documentation](https://github.com/box/box-java-sdk) for the Java client for details on how to configure it. You can also consult our [BoxIntegrationTest](https://github.com/fs2-blobstore/fs2-blobstore/blob/master/box/src/test/scala/blobstore/box/BoxStoreIntegrationTest.scala) for some examples.

Box typically require reading credentials from disk. In the example below, we've included reading credentials, thus producing a `F[BoxStore[F]]`:

```scala mdoc:compile-only
import com.box.sdkgen.box.jwtauth.{BoxJWTAuth, JWTConfig}
import com.box.sdkgen.client.BoxClient

import cats.effect.{Async, Sync}
import blobstore.box.BoxStore

def createBoxStore[F[_]: Async]: F[BoxStore[F]] =
  Sync[F].delay {
    val jwtConfig = JWTConfig.fromConfigFile("/foo.txt")
    val auth = new BoxJWTAuth(jwtConfig)
    val client = new BoxClient(auth)
    BoxStore.builder[F](client).unsafe
  }
```

