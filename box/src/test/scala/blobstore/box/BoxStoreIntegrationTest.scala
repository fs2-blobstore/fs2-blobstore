package blobstore
package box

import java.io.{File, FileNotFoundException, FileReader}
import java.time.{Instant, ZoneOffset}

import blobstore.implicits._
import cats.effect.IO
import cats.implicits._
import com.box.sdk.{BoxAPIConnection, BoxConfig, BoxDeveloperEditionAPIConnection, BoxFile, BoxFolder}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Run these with extreme caution. If configured properly, this test as will attempt to write to your Box server.
  * See AbstractStoreTest to see what operations performed here.
  */
@IntegrationTest
class BoxStoreIntegrationTest extends AbstractStoreTest {

  private val log = LoggerFactory.getLogger(getClass)

  val BoxAppKey   = "BOX_TEST_BOX_APP_KEY"
  val BoxDevToken = "BOX_TEST_BOX_DEV_TOKEN"

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

    devToken
      .orElse(fileCredentials)
      .adaptError {
        case _ => new Exception(s"No box credentials found. Please set $BoxDevToken or $BoxAppKey")
      }
      .get
  }

  lazy val rootFolder: BoxFolder#Info = {
    val rootInfo = BoxFolder.getRootFolder(api).asScala.toList.collectFirst {
      case f: BoxFolder#Info if f.getName == authority => f
    }

    rootInfo match {
      case Some(i) => i
      case None    => fail(new Exception(s"Root folder not found: $authority"))
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val threshold = Instant.now().atZone(ZoneOffset.UTC).minusHours(1)

    rootFolder.getResource
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

  override lazy val store: Store[IO] = BoxStore[IO](api, blocker)

  // If your rootFolderId is a safe directory to test under, this root string doesn't matter that much.
  override val authority: String = "BoxStoreTest"

  behavior of "BoxStore"

  it should "expose underlying metadata" in {
    val dirP = dirUrl("expose-underlying")

    writeLocalFile(store, dirP)("abc.txt")
    val subFolderFile = writeLocalFile(store, dirP / "subfolder")("cde.txt")

    @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
    val paths = store
      .asInstanceOf[BoxStore[IO]]
      .listUnderlying(dirP, BoxFile.ALL_FIELDS ++ BoxFolder.ALL_FIELDS, recursive = false)
      .compile
      .toList
      .unsafeRunSync()

    paths.map(_.fileOrFolder).foreach {
      case Left(file)    => Option(file.getCommentCount) mustBe defined
      case Right(folder) => Option(folder.getIsWatermarked) mustBe defined
    }
    (subFolderFile :: paths).map(store.remove).sequence.unsafeRunSync()
  }

}
