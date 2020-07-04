package com.google.cloud.storage.contrib.nio.testing
import com.google.cloud.storage.StorageOptions

// TODO: Remove this when google-cloud-nio updates and implements writeWithResponse
object FixedLocalStorageHelper {
  private val instance = new FixedFakeStorageRpc(true)

  def getOptions: StorageOptions = {
    instance.reset()
    StorageOptions.newBuilder()
      .setProjectId("dummy-project-for-testing")
      .setServiceRpcFactory((_: StorageOptions) => instance)
      .build()
  }
}
