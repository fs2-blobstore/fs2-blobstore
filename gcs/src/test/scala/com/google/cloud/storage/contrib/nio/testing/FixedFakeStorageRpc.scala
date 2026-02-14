package com.google.cloud.storage.contrib.nio.testing

import com.google.api.client.http.LowLevelHttpRequest
import com.google.api.client.json.gson.GsonFactory
import com.google.api.client.testing.http.MockHttpTransport
import com.google.api.client.util.DateTime
import com.google.api.services.storage.Storage
import com.google.api.services.storage.model.StorageObject
import com.google.cloud.storage.spi.v1.{RpcBatch, StorageRpc}

import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap

class FixedFakeStorageRpc(throwIfOption: Boolean) extends FakeStorageRpc(throwIfOption) {
  metadata = new ConcurrentHashMap[String, StorageObject]()
  contents = new ConcurrentHashMap[String, Array[Byte]]()
  futureContents = new ConcurrentHashMap[String, Array[Byte]]()

  private def now(): DateTime = new DateTime(System.currentTimeMillis())

  private def fullname(so: StorageObject): String =
    s"http://localhost:65555/b/${so.getBucket}/o/${so.getName}"

  override def getStorage: Storage = new Storage.Builder(
    new FakeStorageRpcHttpTransport,
    new GsonFactory,
    _ => {}
  ).setApplicationName("fs2-blobstore-test")
    .build()

  override def create(
    obj: StorageObject,
    content: InputStream,
    options: java.util.Map[StorageRpc.Option, ?]
  ): StorageObject = {
    obj.setTimeCreated(now())
    super.create(obj, content, options)
  }

  override def patch(
    storageObject: StorageObject,
    options: java.util.Map[StorageRpc.Option, ?]
  ): StorageObject = {
    val key      = fullname(storageObject)
    val existing = metadata.get(key)
    Option(existing).foreach(_.setUpdated(now()))
    existing
  }

  override def reset(): Unit = {
    metadata = new ConcurrentHashMap[String, StorageObject]()
    contents = new ConcurrentHashMap[String, Array[Byte]]()
  }

  override def createBatch(): RpcBatch = new FakeRpcBatch(this)

  private class FakeStorageRpcHttpTransport extends MockHttpTransport {
    override def buildRequest(method: String, url: String): LowLevelHttpRequest = create(method, url)
  }
}
