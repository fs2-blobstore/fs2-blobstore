/*
Copyright 2018 LendUp Global, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package blobstore
package s3

import java.net.URI
import com.dimafeng.testcontainers.GenericContainer
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

class S3StoreS3MockTest extends AbstractS3StoreTest {

  override val container: GenericContainer = GenericContainer(
    dockerImage = "adobe/s3mock",
    exposedPorts = List(9090)
  )

  override def client: S3AsyncClient = S3AsyncClient
    .builder()
    .region(Region.US_EAST_1)
    .endpointOverride(URI.create(s"http://${container.containerIpAddress}:${container.mappedPort(9090)}"))
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("a", "s")))
    .build()

  it should "handle files with trailing / in name" in {
    val dir      = dirUrl("trailing-slash")
    val filePath = dir / "file-with-slash/"

    store.put("test", filePath).compile.drain.unsafeRunSync()

    val entities = s3Store
      .listUnderlying(dir, fullMetadata = false, expectTrailingSlashFiles = true, recursive = false)
      .compile
      .toList
      .unsafeRunSync()

    entities.foreach { listedPath =>
      listedPath.fileName mustBe Some("file-with-slash/")
      listedPath.isDir mustBe false
    }

    store.getContents(filePath).unsafeRunSync() mustBe "test"

    store.remove(filePath).unsafeRunSync()

    store.list(dir).compile.toList.unsafeRunSync() mustBe Nil
  }

}
