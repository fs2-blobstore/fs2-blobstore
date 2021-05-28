# fs2-blobstore

[![CI](https://github.com/fs2-blobstore/fs2-blobstore/workflows/CI/badge.svg)](https://github.com/fs2-blobstore/fs2-blobstore/actions?query=workflow:CI)
[![Release](https://github.com/fs2-blobstore/fs2-blobstore/workflows/Release/badge.svg)](https://github.com/fs2-blobstore/fs2-blobstore/actions?query=workflow:Release)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.fs2-blobstore/core_2.12/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.fs2-blobstore%22)
[![codecov](https://codecov.io/gh/fs2-blobstore/fs2-blobstore/branch/master/graph/badge.svg)](https://codecov.io/gh/fs2-blobstore/fs2-blobstore)
[![Join the chat at https://gitter.im/fs2-blobstore/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/fs2-blobstore/Lobby)

<!-- To generate: npm install -g doctoc; doctoc --notitle --maxlevel 3 README.md 
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Installing](#installing)
- [Example](#example)
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
  - [AzureStore](#azurestore)
  - [BoxStore](#boxstore)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


Minimal, idiomatic, Scala interface based on [fs2](https://fs2.io) for blob- and file store implementations. This library provides sources as sinks for a variety of stores (S3, GCS, Azure, SFTP, Box) that you can compose to fs2 programs.

## Installing

fs2-blobstore is published to maven central, add to build.sbt:

```
libraryDependencies ++= Seq(
  "com.github.fs2-blobstore" %% "core"  % "0.7.3",
  "com.github.fs2-blobstore" %% "sftp"  % "0.7.3",
  "com.github.fs2-blobstore" %% "s3"    % "0.7.3",
  "com.github.fs2-blobstore" %% "gcs"   % "0.7.3",
  "com.github.fs2-blobstore" %% "azure" % "0.7.3",
  "com.github.fs2-blobstore" %% "box"   % "0.7.3"
)
```

* `core` module has minimal dependencies and only provides `FileStore` implementation.
* `sftp` module provides `SftpStore` and depends on [Jsch client](http://www.jcraft.com/jsch/). 
* `s3` module provides `S3Store` and depends on [AWS S3 SDK V2](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/)
* `gcs` module provides `GcsStore` and depends on [Google Cloud Storage SDK](https://github.com/googleapis/java-storage)
* `azure` module provides `AzureStore` and depends on [Azure Storage SDK Client library for Java](https://docs.microsoft.com/en-us/java/api/overview/azure/storage)
* `box` module provides `BoxStore` and depends on the [Box SDK for Java](https://github.com/box/box-java-sdk/)

## Example

Upload all files under a S3 prefix to a SFTP server, filter blank lines and run 20 uploads in parallel. 

```scala
import blobstore.Path
import blobstore.s3.S3Store
import blobstore.sftp.SftpStore
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import com.jcraft.jsch.{JSch, Session}
import software.amazon.awssdk.services.s3.S3AsyncClient
import fs2.Stream

object Example extends IOApp {

  val connectSftp: IO[Session] = IO {
    val jsch = new JSch()
    jsch.getSession("sftp.domain.com")
  }

  val s3Client: S3AsyncClient = S3AsyncClient.builder().build()

  /**
    * Transfer 20 files in parallel and filter blank lines
    */
  def logic(s3: S3Store[IO], sftp: SftpStore[IO]): Stream[IO, Unit] =
    s3.list(Path("s3://example-bucket/foo/"))
      .map { path: Path =>
        s3.get(path, chunkSize = 32 * 1024)
          .through(fs2.text.utf8Decode)
          .through(fs2.text.lines)
          .filter(_.nonEmpty)
          .intersperse("\n")
          .through(fs2.text.utf8Encode)
          .through(sftp.put(Path(s"upload/$path")))
      }
      .parJoin(maxOpen = 20)

  override def run(args: List[String]): IO[ExitCode] = {
    Blocker[IO].use { blocker =>
      val program = for {
        s3   <- Stream.eval(S3Store[IO](s3Client))
        sftp <- SftpStore(absRoot = "", connectSftp, blocker)
        _    <- logic(s3, sftp)
      } yield ExitCode.Success

      program.compile.lastOrError
    }
  }
}
```

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
represented as a sink of byte so that any stream of bytes can be piped 
into it to upload data to storage.

### Path Abstraction

[Path](core/src/main/scala/blobstore/Path.scala) is an abstraction for referencing files and folders in Stores:

```scala
trait Path {
  def root: Option[String]
  def pathFromRoot: cats.data.Chain[String]
  def fileName: Option[String]
  def size: Option[Long]
  def isDir: Option[Boolean]
  def lastModified: Option[java.time.Instant]
}
```
 
Usually concrete store would have specific implementation of this trait exposing some store-specific metadata, but all stores should accept any implementation of `Path`. 
Store might apply some optimizations given its specific `Path`, although it's not a requirement.
There is a default most generic implementation of `Path` – [BasePath](core/src/main/scala/blobstore/BasePath.scala), it's used for instance when path is constructed from string.    

By importing implicit `PathOps` into the scope you can make use of path composition `/`
and `filename` function that returns the substring of the path's key after the last path
separator.

**NOTE:** a good improvement to the path abstraction would be to handle OS specific 
separators when referring to filesystem paths.

### Implicit Ops

`import blobstore.implicits._`

[StoreOps](core/src/main/scala/blobstore/StoreOps.scala) and 
[PathOps](core/src/main/scala/blobstore/PathOps.scala) provide functionality on 
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
object storage server), [Azurite](https://github.com/Azure/Azurite) (A lightweight server clone of Azure Storage) and SFTP containers and run all tests not annotated as 
`@IntegrationTest`. GCS is tested against the in-memory 
[`local Storage`](https://github.com/googleapis/google-cloud-java/blob/master/TESTING.md#testing-code-that-uses-storage)

Yes, we understand `SftpStoreTest`, `AzureStoreTest` and `S3StoreTest` are also _integration tests_ 
because they connect to external services, but we don't mark them as such because 
we found these containers that allow to run them along unit tests, and we want to 
exercise as much of the store code as possible.  

Currently, tests for `BoxStore` are annotated with `@IntegrationTest` because we
have not found a box docker image. To run `BoxStore` integration tests locally
you need to provide `BOX_TEST_BOX_DEV_TOKEN`.

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

```scala
import blobstore.{Path, Store}
import blobstore.fs.FileStore
import java.nio.file.Paths
import cats.effect.{Blocker, ExitCode, IO, IOApp}

object FileStoreExample extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = Blocker[IO].use { blocker =>
    val store: Store[IO] = FileStore[IO](Paths.get("tmp/"), blocker)
    store.list(Path("/")).compile.toList.as(ExitCode.Success)
  }
}
```

### S3Store

[S3Store](s3/src/main/scala/blobstore/s3/S3Store.scala) backed by 
[AWS SDK V2](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/welcome.html). It requires a configured instance of `S3AsyncClient`:
   
```scala
import software.amazon.awssdk.services.s3.S3AsyncClient
import blobstore.s3.S3Store

object S3Example extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val s3 = {
        val builder = S3AsyncClient.builder()
        // Configure client
        // ...
        builder.build()
    }
    S3Store[IO](s3).flatMap{store =>
        store.list(Path("s3://foo/bar/")).compile.toList
        // ...
    }.attempt.map(_.fold(_ => ExitCode.Error, _ => ExitCode.Success))
  }
}
```

### GcsStore

[GcsStore](gcs/src/main/scala/blobstore/gcs/GcsStore.scala) backed by 
[Google Cloud Storage](https://github.com/googleapis/java-storage). It requires a configured `Storage`:

```scala
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

```scala
import blobstore.sftp.SftpStore
import com.jcraft.jsch.{JSch, Session}

object SftpExample extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = Blocker[IO].use { blocker =>
    val session: IO[Session] = IO {
      val jsch = new JSch()
      jsch.getSession("sftp.domain.com")
    }

    val store: fs2.Stream[IO, SftpStore[IO]] = SftpStore("", session, blocker)

    store.flatMap(_.list(Path("."))).compile.toList.as(ExitCode.Success)
  }
}
```

### AzureStore

[AzureStore](azure/src/main/scala/blobstore/azure/AzureStore.scala) backed by [Azure Storage Blob client library for Java](https://github.com/Azure/azure-sdk-for-java/tree/master/sdk/storage/azure-storage-blob). It requires a configured instance of `BlobServiceAsyncClient`:

```scala
import com.azure.storage.blob.BlobServiceClientBuilder
import blobstore.azure.AzureStore

object AzureExample extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val azure = {
        val builder = new BlobServiceClientBuilder()
        // Configure client
        // ...
        builder.buildAsyncClient()
    }
    AzureStore[IO](azure).flatMap{store =>
        store.list(Path("container://foo/bar/")).compile.toList
        // ...
    }.attempt.map(_.fold(_ => ExitCode.Error, _ => ExitCode.Success))
  }
}
```

### BoxStore

[BoxStore](box/src/main/scala/blobstore/box/BoxStore.scala) backed by
a [BoxAPIConnection](https://github.com/box/box-java-sdk/blob/master/src/main/java/com/box/sdk/BoxAPIConnection.java),
which has multiple options for authentication. This requires that you have a Box app set up already. 
See [Box SDK documentation](https://github.com/box/box-java-sdk) for more details:

```scala
import blobstore.box.BoxStore
import com.box.sdk.BoxAPIConnection

object BoxExample extends IOApp {
  
  override def run(args: List[String]): IO[ExitCode] = Blocker[IO].use { blocker =>
    val api = new BoxAPIConnection("myDeveloperToken")
    val store: Store[IO] = BoxStore[IO](api, blocker)
  
    store.list(Path(".")).compile.toList.as(ExitCode.Success)
  }
}
```

