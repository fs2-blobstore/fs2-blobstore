package blobstore
package box

import java.io.{File, FileNotFoundException, FileReader}
import java.time.{Instant, ZoneOffset}
import blobstore.url.Authority
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.box.sdk.{BoxAPIConnection, BoxConfig, BoxDeveloperEditionAPIConnection, BoxFile, BoxFolder}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Run these with extreme caution. If configured properly, this test as will attempt to write to your Box server.
  * See AbstractStoreTest to see what operations performed here.
  */
@IntegrationTest
class BoxStoreIntegrationTest extends AbstractStoreTest[BoxPath] {

  private val log = LoggerFactory.getLogger(getClass)

  val BoxAppKey   = "BOX_TEST_BOX_APP_KEY"
  val BoxDevToken = "BOX_TEST_BOX_DEV_TOKEN"

  override val scheme = "https"
  // If your rootFolderId is a safe directory to test under, this root string doesn't matter that much.
  override val authority: Authority = Authority.unsafe("foo")

  private lazy val boxStore: BoxStore[IO]    = BoxStore[IO](api)
  override def mkStore(): Store[IO, BoxPath] = boxStore.lift

  val rootFolderName = "BoxStoreTest"

  lazy val api: BoxAPIConnection = {
    val fileCredentialName = sys.env.getOrElse(BoxAppKey, "box_appkey.json")
    lazy val fileCredentials = Option(getClass.getClassLoader.getResource(fileCredentialName))
      .toRight(new FileNotFoundException(fileCredentialName))
      .toTry
      .map(u => new FileReader(new File(u.toURI)))
      .map(BoxConfig.readFrom)
      .map(BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection)
      .flatTap(_ => Try(log.info(s"Authenticated with credentials file $fileCredentialName")))

    val devToken = sys.env
      .get(BoxDevToken)
      .toRight(new Exception(s"Environment variable not set: $BoxDevToken"))
      .toTry
      .map(new BoxAPIConnection(_))
      .flatTap(_ => Try(log.info("Authenticated with dev token")))

    devToken.orElse(fileCredentials) match {
      case Failure(e) => cancel(s"No box credentials found. Please set $BoxDevToken or $BoxAppKey", e)
      case Success(c) => c
    }
  }

  lazy val rootFolder: BoxFolder#Info = {
    val list = BoxFolder.getRootFolder(api).asScala.toList
    val rootInfo = list.collectFirst {
      case f: BoxFolder#Info if f.getName == rootFolderName => f
    }

    rootInfo match {
      case Some(i) => i
      case None    => fail(new Exception(s"Root folder not found: $authority"))
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val threshold = Instant.now().atZone(ZoneOffset.UTC).minusHours(1)

    rootFolder.getResource // scalafix:ok
      .getChildren(BoxFolder.ALL_FIELDS: _*)
      .asScala
      .toList
      .filter(_.getCreatedAt.toInstant.atZone(ZoneOffset.UTC).isBefore(threshold))
      .foreach {
        case i: BoxFolder#Info =>
          log.info(s"Deleting outdated test folder ${i.getName}")
          Try(i.getResource.delete(true))
        case i: BoxFile#Info =>
          log.info(s"Deleting outdated test file ${i.getName}")
          Try(i.getResource.delete())
        case i => log.info(s"Ignoring old item $i")
      }
  }

  behavior of "BoxStore"

  it should "pick up correct storage class" in {
    val dir     = dirUrl("trailing-slash")
    val fileUrl = dir / "file"

    store.putContent(fileUrl, "test").unsafeRunSync()

    store.list(fileUrl).map { u =>
      u.path.storageClass mustBe None
    }.compile.lastOrError.unsafeRunSync()

    val storeGeneric: Store.Generic[IO] = store

    storeGeneric.list(fileUrl).map { u =>
      u.path.storageClass mustBe None
    }.compile.lastOrError.unsafeRunSync()
  }

  it should "expose underlying metadata" in {
    val dirP = dirUrl("expose-underlying")

    writeFile(store, dirP)("abc.txt")
    val subFolderFile = writeFile(store, dirP / "subfolder")("cde.txt")

    val paths = boxStore
      .listUnderlying(dirP.path, BoxFile.ALL_FIELDS ++ BoxFolder.ALL_FIELDS, recursive = false)
      .compile
      .toList
      .unsafeRunSync()

    paths.map(_.representation.fileOrFolder).foreach {
      case Left(file)    => Option(file.getCommentCount) mustBe a[Some[_]]
      case Right(folder) => Option(folder.getIsWatermarked) mustBe a[Some[_]]
    }
    (subFolderFile.path :: paths.map(_.plain)).map(boxStore.remove(_, false)).sequence.unsafeRunSync()
  }

}
