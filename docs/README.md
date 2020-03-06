# fs2-blobstore

[![Build Status](https://api.travis-ci.org/fs2-blobstore/fs2-blobstore.svg?branch=master)](https://travis-ci.org/fs2-blobstore/fs2-blobstore)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.fs2-blobstore/core_2.12/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.fs2-blobstore%22)
[![codecov](https://codecov.io/gh/fs2-blobstore/fs2-blobstore/branch/master/graph/badge.svg)](https://codecov.io/gh/fs2-blobstore/fs2-blobstore)
[![Join the chat at https://gitter.im/fs2-blobstore/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/fs2-blobstore/Lobby)

<!-- To generate: npm install -g doctoc; doctoc --notitle --maxlevel 3 README.md 
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Installing](#installing)
- [Internals](#internals)
  - [Store Abstraction](#store-abstraction)
  - [Path Abstraction](#path-abstraction)
  - [Implicit Ops](#implicit-ops)
  - [Tests](#tests)
- [Store Implementations](#store-implementations)
  - [FileStore](#filestore)
  - [S3Store](#s3store)
  - [GcsStore](#gcsstore)
  - [SftpStore](#sftpstore)
  - [BoxStore](#boxstore)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


Minimal, idiomatic, stream-based Scala interface for key/value store implementations.
It provides abstractions for S3-like key/value store backed by different persistence 
mechanisms (i.e. S3, FileSystem, sftp, etc).

## Installing

fs2-blobstore is deployed to maven central, add to build.sbt:

```sbtshell
libraryDependencies ++= Seq(
  "com.github.fs2-blobstore" %% "core" % "@VERSION@",
  "com.github.fs2-blobstore" %% "sftp" % "@VERSION@",
  "com.github.fs2-blobstore" %% "s3"   % "@VERSION@",
  "com.github.fs2-blobstore" %% "gcs"  % "@VERSION@",
)
```

`core` module has minimal dependencies and only provides `FileStore` implementation.
`sftp` module provides `SftpStore` and depends on [Jsch client](http://www.jcraft.com/jsch/). 
`s3` module provides `S3Store` and depends on [AWS S3 SDK](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3.html)
`gcs` module provides `GcsStore` and depends on [Google Cloud Storage SDK](https://github.com/googleapis/java-storage)


## Internals
### Store Abstraction

The goal of the [Store interface](core/src/main/scala/blobstore/Store.scala) is to 
have a common representation of key/value functionality (get, put, list, etc) as 
streams that can be composed, transformed and piped just like any other `fs2.Stream` 
or `fs2.Pipe` regardless of the underlying storage mechanism.

This is especially useful for unit testing if you are building a S3, GCS or SFTP backed 
system as you can provide a filesystem based implementation for tests that is 
guaranteed to work the same way as you production environment.

The three main activities in a key/value store are modeled like:

```scala
def list(path: Path): fs2.Stream[F, Path]
def get(path: Path, chunkSize: Int): fs2.Stream[F, Byte]
def put(path: Path): Pipe[F, Byte, Unit] 
```  

Note that `list` and `get` are modeled as streams since they are reading 
(potentially) very large amounts of data from storage, while `put` is 
represented as a sink of byte so that any stream of bytes can by piped 
into it to upload data to storage.

### Path Abstraction

`blobstore.Path` is the representation of `key` in the key/value store. The key 
representation is based on S3 that has a `root` (or bucket) and a `key` string.

When functions in the `Store` interface that receive a `Path` should assume that only
root and key values are set, there is no guarantee that the other attributes of `Path`
would be filled: size, isDir, lastModified. On the other hand, when a `Store` implements
the list function, it is expected that all 3 fields will be present in the response.

By importing implicit `PathOps` into the scope you can make use of path composition `/`
and `filename` function that returns the substring of the path's key after the last path
separator.

**NOTE:** a good improvement to the path abstraction would be to handle OS specific 
separators when referring to filesystem paths.

### Implicit Ops

`import blobstore.implicits._`

[StoreOps](core/src/main/scala/blobstore/StoreOps.scala) and 
[PathOps](core/src/main/scala/blobstore/Path.scala) provide functionality on 
both `Store` and `Path` for commonly performed tasks (i.e. upload/download a 
file from/to local filesystem, collect all returned paths when listing, composing 
paths or extracting filename of the path).

Most of these common tasks encapsulate stream manipulation and provide a simpler 
interface that return the corresponding effect monad.  These are also very good 
examples of how to use blobstore streams and sink in different scenarios.

### Tests

All store implementations must support and pass the suite of tests in 
[AbstractStoreTest](core/src/test/scala/blobstore/AbstractStoreTest.scala). 
It is expected that each store implementation (like s3, sftp, file) should 
contain the `Store` implementation and at least one test suite that inherits 
from `AbstractStoreTest` and overrides store and root attributes:

```scala
import cats.effect.IO
import blobstore.Store
import blobstore.AbstractStoreTest

class MyStoreImplTest extends AbstractStoreTest {
  override val store: Store[IO] = MyStoreImpl( ... )
  override val root: String = "my_store_impl_tests"
}
```  

This test suite will guarantee that basic operations are supported properly and 
consistent with all other `Store` implementations.

**Running Tests:**

Tests are set up to run via docker-compose:

```bash
docker-compose run --rm sbt "testOnly * -- -l blobstore.IntegrationTest"
```

This will start a [minio](https://www.minio.io/docker.html) (Amazon S3 compatible 
object storage server) and SFTP containers and run all tests not annotated as 
`@IntegrationTest`. GCS is tested against the in-memory 
[`local Storage`](https://github.com/googleapis/google-cloud-java/blob/master/TESTING.md#testing-code-that-uses-storage)

Yes, we understand `SftpStoreTest` and `S3StoreTest` are also _integration tests_ 
because they connect to external services, but we don't mark them as such because 
we found these containers that allow to run them along unit tests and we want to 
exercise as much of the store code as possible.  

Currently, tests for `BoxStore` are annotated with `@IntegrationTest` because we
have not found a box docker image. To run `BoxStore` integration tests locally
you need to provide env vars for `BOX_TEST_BOX_DEV_TOKEN` and `BOX_TEST_ROOT_FOLDER_ID`.

Run box tests with:

```bash
sbt box/test
```

**Note:** this will exercise `AbstractStoreTest` tests against your box.com account.

## Store Implementations

### FileStore

[FileStore](fs/src/main/scala/blobstore/fs/FileStore.scala) backed by local
FileSystem. FileStore is provided as part of core module because it doesn't
include any additional dependencies and it is used as the default source store
in TransferOps tests. It only requires root path in the local file system:

```scala mdoc
import blobstore.{Path, Store}
import blobstore.fs.FileStore
import java.nio.file.Paths
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import cats.syntax.functor._

object FileStoreExample extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = Blocker[IO].use { blocker =>
    val store: Store[IO] = FileStore[IO](Paths.get("tmp/"), blocker)
    store.list(Path("/")).compile.toList.as(ExitCode.Success)
  }
}
```

### S3Store

[S3Store](s3/src/main/scala/blobstore/s3/S3Store.scala) backed by 
[AWS S3](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3.html). 
It requires a computation resulting in a `TransferManager`. The resulting stream shuts down the allocated
`TransferManager` on stream completion:
   
```scala mdoc
import blobstore.s3.S3Store
import com.amazonaws.services.s3.transfer.TransferManagerBuilder

object S3Example extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = Blocker[IO].use { blocker =>
    val tm = IO(TransferManagerBuilder.standard().build())
    val store: fs2.Stream[IO, S3Store[IO]] = S3Store(transferManager = tm, encrypt = false, blocker = blocker)
    
    store.flatMap(_.list(Path("s3://foo/bar/"))).compile.toList.as(ExitCode.Success)
  }
}
```

### GcsStore

[GcsStore](gcs/src/main/scala/blobstore/gcs/GcsStore.scala) backed by 
[Google Cloud Storage](https://github.com/googleapis/java-storage). It requires a configured `Storage`:

```scala mdoc
import blobstore.gcs.GcsStore
import com.google.cloud.storage.{Storage, StorageOptions}

object GcsExample extends IOApp {
   def run(args: List[String]): IO[ExitCode] = Blocker[IO].use { blocker =>
     val storage: Storage = StorageOptions.getDefaultInstance.getService
     val store: Store[IO] = GcsStore(storage, blocker, List.empty)
     store.list(Path("gs://foo/bar")).compile.drain.as(ExitCode.Success)
   }
}
``` 

### SftpStore

[SftpStore](sftp/src/main/scala/blobstore/sftp/SftpStore.scala) backed by 
SFTP server with [Jsch client](http://www.jcraft.com/jsch/). It requires a computation resulting in a `Session`:

```scala mdoc
import blobstore.sftp.SftpStore
import com.jcraft.jsch.{JSch, Session}

object SftpExample extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = Blocker[IO].use { blocker =>
    val session: IO[Session] = IO {
      val jsch = new JSch()
      val session = jsch.getSession("sftp.domain.com")
      session.connect()

      session
    }

    val store: fs2.Stream[IO, SftpStore[IO]] = SftpStore("", session, blocker)

    store.flatMap(_.list(Path("."))).compile.toList.as(ExitCode.Success)
  }
}
```

### BoxStore

[BoxStore](box/src/main/scala/blobstore/box/BoxStore.scala) backed by
a [BoxAPIConnection](https://github.com/box/box-java-sdk/blob/master/src/main/java/com/box/sdk/BoxAPIConnection.java),
which has multiple options for authentication. This requires that you have a Box app set up already. 
See [Box SDK documentation](https://github.com/box/box-java-sdk) for more details:

```scala mdoc
import blobstore.box.BoxStore
import com.box.sdk.BoxAPIConnection

object BoxExample extends IOApp {
  
  override def run(args: List[String]): IO[ExitCode] = Blocker[IO].use { blocker =>
    val api = new BoxAPIConnection("myDeveloperToken")
    val store: Store[IO] = BoxStore[IO](api, "rootFolderId", blocker)
  
    store.list(Path(".")).compile.toList.as(ExitCode.Success)
  }
}
```

