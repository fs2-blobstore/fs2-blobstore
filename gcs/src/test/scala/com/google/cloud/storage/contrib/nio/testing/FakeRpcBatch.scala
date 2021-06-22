package com.google.cloud.storage.contrib.nio.testing

import java.util
import com.google.api.services.storage.model.StorageObject
import com.google.cloud.storage.spi.v1.{RpcBatch, StorageRpc}

class FakeRpcBatch(storageRpc: StorageRpc) extends RpcBatch {
  val deletions =
    collection.mutable.ListBuffer[(StorageObject, RpcBatch.Callback[Void], util.Map[StorageRpc.Option, _])]()

  def addDelete(
    storageObject: StorageObject,
    callback: RpcBatch.Callback[Void],
    options: util.Map[StorageRpc.Option, _]
  ): Unit = deletions.append((storageObject, callback, options))

  def addPatch(
    storageObject: StorageObject,
    callback: RpcBatch.Callback[StorageObject],
    options: util.Map[StorageRpc.Option, _]
  ): Unit = ???

  def addGet(
    storageObject: StorageObject,
    callback: RpcBatch.Callback[StorageObject],
    options: util.Map[StorageRpc.Option, _]
  ): Unit = ???

  def submit(): Unit = deletions.foreach { case (o, callback, options) =>
    storageRpc.delete(o, options)
    callback.onSuccess(null) // scalafix:ok
  }
}
