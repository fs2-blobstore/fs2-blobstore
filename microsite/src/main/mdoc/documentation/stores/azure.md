---
layout: docs
title: Azure
---

# Azure

This store is used to interface with Azure's Blob Storage. Head over to the official [documentation](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-java?tabs=powershell) for the Java client for details on how to configure it. Here is a vanilla setup that uses an account key available at runtime:

```scala mdoc:silent
import com.azure.storage.blob.{BlobServiceAsyncClient, BlobServiceClientBuilder}

val azure: BlobServiceAsyncClient = new BlobServiceClientBuilder()
  .connectionString(
    s"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  )
  .buildAsyncClient()
```

