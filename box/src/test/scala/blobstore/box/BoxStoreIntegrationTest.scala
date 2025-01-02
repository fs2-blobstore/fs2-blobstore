package blobstore
package box

import java.io.FileNotFoundException
import java.time.{Instant, ZoneOffset}
import blobstore.url.{Authority, Url}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.box.sdkgen.box.developertokenauth.BoxDeveloperTokenAuth
import com.box.sdkgen.box.jwtauth.{BoxJWTAuth, JWTConfig}
import com.box.sdkgen.client.BoxClient
import com.box.sdkgen.managers.folders.DeleteFolderByIdQueryParams
import com.box.sdkgen.schemas.folderfull.FolderFull
import com.box.sdkgen.schemas.items.Items
import org.slf4j.LoggerFactory

import com.box.sdkgen.managers.files.UpdateFileByIdRequestBody

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/** Run these with extreme caution. If configured properly, this test as will attempt to write to your Box server. See
  * AbstractStoreTest to see what operations performed here.
  */
@IntegrationTest
class BoxStoreIntegrationTest extends AbstractStoreTest[BoxPath] {

  private val log = LoggerFactory.getLogger(getClass)

  val BoxAppKey   = "BOX_TEST_BOX_APP_KEY"
  val BoxDevToken = "BOX_TEST_BOX_DEV_TOKEN"

  override val scheme = "https"
  // If your rootFolderId is a safe directory to test under, this root string doesn't matter that much.
  override val authority: Authority = Authority.unsafe("foo")

  private lazy val boxStore: BoxStore[IO]    = BoxStore.builder[IO](boxClient).unsafe
  override def mkStore(): Store[IO, BoxPath] = boxStore.lift

  val rootFolderName = "BoxStoreTest"

  lazy val boxClient: BoxClient = {
    val fileCredentialName   = sys.env.getOrElse(BoxAppKey, "box_appkey.json")
    lazy val fileCredentials = Option(getClass.getClassLoader.getResource(fileCredentialName))
      .toRight(new FileNotFoundException(fileCredentialName))
      .toTry
      .map(u => java.nio.file.Paths.get(u.toURI).toString)
      .map(JWTConfig.fromConfigFile)
      .map(new BoxJWTAuth(_))
      .map(new BoxClient(_))
      .flatTap(_ => Try(log.info(show"Authenticated with credentials file $fileCredentialName")))

    val devToken = sys.env
      .get(BoxDevToken)
      .toRight(new Exception(show"Environment variable not set: $BoxDevToken"))
      .toTry
      .map(new BoxDeveloperTokenAuth(_))
      .map(new BoxClient(_))
      .flatTap(_ => Try(log.info("Authenticated with dev token")))

    devToken.orElse(fileCredentials) match {
      case Failure(e) => cancel(show"No box credentials found. Please set $BoxDevToken or $BoxAppKey", e)
      case Success(c) => c
    }
  }

  lazy val rootFolder: FolderFull = {
    val items    = boxClient.folders.getFolderItems("0")
    val rootInfo = items.getEntries.asScala.collectFirst {
      case item if item.isFolderFull && item.getFolderFull.getName == rootFolderName => item.getFolderFull
    }

    rootInfo match {
      case Some(i) => i
      case None    => fail(new Exception(show"Root folder not found: $authority"))
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val threshold    = Instant.now().atZone(ZoneOffset.UTC).minusHours(1)
    val items: Items = boxClient.folders.getFolderItems(rootFolder.getId)
    items.getEntries.asScala.toList
      .filter { item =>
        val createdAt = if (item.isFileFull) Option(item.getFileFull.getCreatedAt)
        else if (item.isFolderFull) Option(item.getFolderFull.getCreatedAt)
        else None
        createdAt.exists(_.toInstant.atZone(ZoneOffset.UTC).isBefore(threshold))
      }
      .foreach {
        case item if item.isFolderFull =>
          log.info(show"Deleting outdated test folder ${item.getFolderFull.getName}")
          val _ = Try {
            val params = new DeleteFolderByIdQueryParams.Builder().recursive(true).build()
            boxClient.folders.deleteFolderById(item.getFolderFull.getId, params)
          }
        case item if item.isFileFull =>
          log.info(show"Deleting outdated test file ${item.getFileFull.getName}")
          val _ = Try(boxClient.files.deleteFileById(item.getFileFull.getId))
        case item => log.info(show"Ignoring old item ${item.getName}")
      }
  }

  override def modifyFile(url: Url.Plain): IO[Unit] =
    boxStore.stat(url.path).flatMap {
      case Some(p) =>
        val fileId = p.representation.file.map(_.getId).orNull
        IO.blocking {
          val body = new UpdateFileByIdRequestBody.Builder().description("modified").build()
          boxClient.files.updateFileById(fileId, body)
        }.void
      case None => IO.raiseError(new IllegalArgumentException(show"File not found: $url"))
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
      .listUnderlying(dirP.path, List("comment_count", "watermark_info"), recursive = false)
      .compile
      .toList
      .unsafeRunSync()

    paths.map(_.representation.fileOrFolder).foreach {
      case Left(file)    => Option(file.getCommentCount) mustBe a[Some[?]]
      case Right(folder) => Option(folder.getWatermarkInfo) mustBe a[Some[?]]
    }
    (subFolderFile.path :: paths.map(_.plain)).map(boxStore.remove(_, false)).sequence.unsafeRunSync()
  }

}
