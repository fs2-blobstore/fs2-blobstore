package com.google.cloud.storage.contrib.nio.testing

import com.google.api.services.storage.model.StorageObject
import com.google.cloud.storage.spi.v1.RpcBatch

import java.util.concurrent.ConcurrentHashMap

class FixedFakeStorageRpc(throwIfOption: Boolean) extends FakeStorageRpc(throwIfOption) {
  metadata = new ConcurrentHashMap[String, StorageObject]()
  contents = new ConcurrentHashMap[String, Array[Byte]]()
  futureContents = new ConcurrentHashMap[String, Array[Byte]]()

  override def reset(): Unit = {
    metadata = new ConcurrentHashMap[String, StorageObject]()
    contents = new ConcurrentHashMap[String, Array[Byte]]()
  }

  override def createBatch(): RpcBatch = new FakeRpcBatch(this)
}
