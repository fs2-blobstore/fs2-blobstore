package blobstore
package box

import java.io.{File, FileNotFoundException, FileReader}
import blobstore.url.{Authority, Path}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import cats.effect.std.Console
import com.box.sdk.{BoxAPIConnection, BoxConfig, BoxDeveloperEditionAPIConnection, BoxFile, BoxFolder}
import weaver.GlobalRead

import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters._

/** Run these with extreme caution. If configured properly, this test as will attempt to write to your Box server.
  * See AbstractStoreTest to see what operations performed here.
  */
class BoxStoreIntegrationTest(global: GlobalRead) extends AbstractStoreTest[BoxPath, BoxStore[IO]](global) {

  // Box cannot handle parallel execution
  override def maxParallelism: Int = 1

  val rootFolderName = "BoxStoreTest"

  val BoxAppKey   = "BOX_TEST_BOX_APP_KEY"
  val BoxDevToken = "BOX_TEST_BOX_DEV_TOKEN"

  override val scheme: String = "https"

  // If your rootFolderId is a safe directory to test under, this root string doesn't matter that much.
  override val authority: Authority = Authority.unsafe("foo")

  override val fileSystemRoot: Path.Plain = Path(rootFolderName)
  override val testRunRoot: Path.Plain    = Path(s"$rootFolderName/$testRun")

  val developerEditionAPIConnection: IO[BoxDeveloperEditionAPIConnection] =
    IO(sys.env.getOrElse(BoxAppKey, "box_appkey.json"))
      .flatMap { fileCredentialName =>
        IO.fromEither(Option(getClass.getClassLoader.getResource(fileCredentialName))
          .toRight(new FileNotFoundException(fileCredentialName)))
          .flatMap { u =>
            IO(
              BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(
                BoxConfig.readFrom(new FileReader(new File(u.toURI)))
              )
            ).flatTap(_ => Console[IO].println(s"Authenticated with credentials file $fileCredentialName"))
          }
      }

  val tokenConnection: IO[BoxAPIConnection] =
    IO.fromEither(
      Option("1lAEIm9CRrI01nmRAbkYSMcXi1OeczBK")
//      sys.env.get(BoxDevToken)
        .toRight(new Exception(s"Environment variable not set: $BoxDevToken"))
    )
      .flatMap(token => IO(new BoxAPIConnection(token)))
      .flatTap(_ => Console[IO].println("Authenticated with dev token"))

  val connectionResource: Resource[IO, BoxAPIConnection] = Resource.eval {
    developerEditionAPIConnection.handleErrorWith(_ =>
      tokenConnection.handleErrorWith(_ => cancel(s"No box credentials found. Please set $BoxDevToken or $BoxAppKey"))
    )
  }.evalTap { connection =>
    IO {
      connection.setMaxRetryAttempts(10)
    }
  }

  def rootFolder(connection: BoxAPIConnection): IO[BoxFolder#Info] = IO.blocking {
    BoxFolder.getRootFolder(connection)
      .asScala
      .toList
      .collectFirst {
        case f: BoxFolder#Info if f.getName == rootFolderName => f
      }
  }.flatMap {
    case Some(f) => IO.pure(f)
    case None    => IO.raiseError(new Exception(s"Root folder not found: $rootFolderName"))
  }

  def cleanup(rootFolder: BoxFolder#Info): IO[Unit] =
    IO.realTimeInstant.flatMap { now =>
      val threshold = now.minus(1, ChronoUnit.HOURS)
      IO.blocking(rootFolder.getResource.getChildren("created_at").asScala.toList).flatMap(
        _.filter(_.getCreatedAt.toInstant.isBefore(threshold)).traverse_ {
          case i: BoxFile#Info =>
            Console[IO].println(s"Deleting outdated test file ${i.getName}") >>
              IO.blocking(i.getResource.delete())
          case i: BoxFolder#Info =>
            Console[IO].println(s"Deleting outdated test folder ${i.getName}") >>
              IO.blocking(i.getResource.delete(true))
          case i => Console[IO].println(s"Ignoring old item ${i.getName}") >>
              IO.unit
        }
      )
    }

  override def sharedResource: Resource[IO, TestResource[BoxPath, BoxStore[IO]]] = for {
    connection <- connectionResource
    rf         <- Resource.eval(rootFolder(connection))
    _          <- Resource.eval(cleanup(rf))
  } yield {
    val store = BoxStore[IO](connection)
    TestResource(store.lift, store, disableHighRateTests = true)
  }

  test("expose underlying metadata") { (res, log) =>
    val dir = dirUrl("expose-underlying")
    for {
      _ <- writeRandomFile(res.store, log)(dir)
      urls <-
        res.extra.listUnderlying(dir.path, Array("comment_count", "watermark_info"), recursive = false).compile.toList
    } yield {
      urls.map { p =>
        p.representation.fileOrFolder match {
          case Left(file)    => expect(Option(file.getCommentCount).isDefined)
          case Right(folder) => expect(Option(folder.getIsWatermarked).isDefined)
        }
      }.combineAll
    }
  }
}
