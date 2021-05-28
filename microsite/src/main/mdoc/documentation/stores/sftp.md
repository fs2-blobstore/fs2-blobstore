---
layout: docs
title: SFTP
---

# SFTP

This store is used to interface with SFTP servers. It's built on [JSch](http://www.jcraft.com/jsch/) and abstracts a lot of quirks in addition to providing a pure functional interface.

```scala mdoc:silent
import blobstore.sftp.SftpStore
import cats.ApplicativeThrow
import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.jcraft.jsch.JSch

import java.util.Properties

def createStore[F[_]: Async]: Resource[F, SftpStore[F]] = {
  val createSession = ApplicativeThrow[F].catchNonFatal(new JSch().getSession("username", "host")).map { session =>
    session.setTimeout(10000)
    session.setPassword("password")
 
    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session // Let the store connect this session
  }
  
  SftpStore[F](createSession)
}
```

`SftpStore` creates a connection pool for JSch's [`ChannelSftp`](https://epaul.github.io/jsch-documentation/javadoc/com/jcraft/jsch/ChannelSftp.html), this is exposed as a `Resource[F, SftpStore[F]]`. You can control the size of this connection pool through the `SftpStore` constructor.

## Absolute- and rootless paths

Our `Path[A]` implementation is a co-product of `AbsolutePath[A]` and `RootlessPath[A]`,  the path constructor will do the right thing given a `String`:

```scala mdoc
import blobstore.url.Path
import blobstore.url.Path.{AbsolutePath, RootlessPath}

Path("/foo/bar") == AbsolutePath.createFrom("/foo/bar")
Path("foo/bar") == RootlessPath.createFrom("foo/bar")
```

JSch uses SSH as the secure channel, and the semantics of relative paths is defined by SSH. Depending on how the SFTP server is configured, upon login you will either land in `/` (if the server uses chroot) or `/home/username/` (or anywhere else).

Rootless paths such as `foo/bar.txt` are always relative to your home directory under SSH. We don't expose any APIs for changing the current working directory, so `./foo/bar.txt` and `foo/bar.txt` will always refer to the same object. In general, sticking to rootless paths is a good idea as you never risk to refer to anything outside the login directory. 