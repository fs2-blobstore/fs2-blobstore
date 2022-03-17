# fs2-blobstore

[![CI](https://github.com/fs2-blobstore/fs2-blobstore/workflows/CI/badge.svg)](https://github.com/fs2-blobstore/fs2-blobstore/actions?query=workflow:CI)
[![Release](https://github.com/fs2-blobstore/fs2-blobstore/workflows/Release/badge.svg)](https://github.com/fs2-blobstore/fs2-blobstore/actions?query=workflow:Release)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.fs2-blobstore/core_2.12/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.fs2-blobstore%22)
[![codecov](https://codecov.io/gh/fs2-blobstore/fs2-blobstore/branch/master/graph/badge.svg)](https://codecov.io/gh/fs2-blobstore/fs2-blobstore)
[![Join the chat at https://gitter.im/fs2-blobstore/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/fs2-blobstore/Lobby)

Unified Scala interface based on [fs2](https://fs2.io) for hierarhical and flat object stores. This library lets you integrate fs2 programs with various storage technologies such as S3, GCS, Azure Blob Storage, SFTP and Box. It also offers an interface that abstracts over the underlying storage technology, this lets you write fs2 programs that are agnostic to what storage provider files are hosted on.

### Quick Start

```scala
libraryDependencies ++= Seq(
  "com.github.fs2-blobstore" %% "core"  % "<version>",
  "com.github.fs2-blobstore" %% "sftp"  % "<version>",
  "com.github.fs2-blobstore" %% "s3"    % "<version>",
  "com.github.fs2-blobstore" %% "gcs"   % "<version>",
  "com.github.fs2-blobstore" %% "azure" % "<version>",
  "com.github.fs2-blobstore" %% "box"   % "<version>",
) 
```

* `core` module has minimal dependencies and only provides `FileStore` implementation.
* `sftp` module provides `SftpStore` and depends on [Jsch client](http://www.jcraft.com/jsch/).
* `s3` module provides `S3Store` and depends on [AWS S3 SDK V2](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/)
* `gcs` module provides `GcsStore` and depends on [Google Cloud Storage SDK](https://github.com/googleapis/java-storage)
* `azure` module provides `AzureStore` and depends on [Azure Storage SDK Client library for Java](https://docs.microsoft.com/en-us/java/api/overview/azure/storage)
* `box` module provides `BoxStore` and depends on the [Box SDK for Java](https://github.com/box/box-java-sdk/)

Head over to the [microsite](https://fs2-blobstore.github.io/fs2-blobstore/) for documentation
