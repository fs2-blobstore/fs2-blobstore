---
layout: docs
title: Box
---

# Box

This store is used to interface with Box. Head over to the official [documentation](https://github.com/box/box-java-sdk) for the Java client for details on how to configure it. You can also consult our [BoxIntegrationTest](https://github.com/fs2-blobstore/fs2-blobstore/blob/master/box/src/test/scala/blobstore/box/BoxStoreIntegrationTest.scala) for some examples.

Box typically require reading credentials from disk. In the example below, we've included reading credentials, thus producing a `F[BoxStore[F]]`:

```scala mdoc:compile-only
import com.box.sdk.{BoxConfig, BoxDeveloperEditionAPIConnection}

import cats.effect.{Async, Sync}
import fs2.io.file.{Files, Path}
import blobstore.box.BoxStore

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

def createBoxStore[F[_]: Async]: F[BoxStore[F]] = Files.forAsync[F].readAll(Path("/foo.txt"))
  .through(fs2.io.toInputStream)
  .map(new InputStreamReader(_, StandardCharsets.UTF_8))
  .evalMap(r => Sync[F].delay(BoxConfig.readFrom(r)))
  .map(BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection)
  .map(BoxStore.builder[F](_).unsafe)
  .compile
  .lastOrError
```
 
