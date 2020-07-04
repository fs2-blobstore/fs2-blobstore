package com.google.cloud.storage.contrib.nio.testing

import java.util.concurrent.ConcurrentHashMap

import com.google.api.services.storage.model.StorageObject

// TODO: Remove this when google-cloud-nio updates and implements writeWithResponse
class FixedFakeStorageRpc(throwIfOption: Boolean) extends FakeStorageRpc(throwIfOption) {
  metadata = new ConcurrentHashMap[String, StorageObject]()
  contents = new ConcurrentHashMap[String, Array[Byte]]()
  futureContents = new ConcurrentHashMap[String, Array[Byte]]()

  override def writeWithResponse(
    uploadId: String,
    toWrite: Array[Byte],
    toWriteOffset: Int,
    destOffset: Long,
    length: Int,
    last: Boolean
  ): StorageObject = {
    write(uploadId, toWrite, toWriteOffset, destOffset, length, last)
    null
  }
}
