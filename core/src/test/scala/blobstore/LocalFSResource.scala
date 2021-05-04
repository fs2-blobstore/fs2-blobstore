package blobstore

import blobstore.fs.{FileStore, NioPath}
import blobstore.url.{Authority, Path, Url}
import cats.syntax.all._
import cats.effect.{IO, Resource}
import fs2.io.file.Files
import weaver._

import java.nio.file.Paths
import scala.util.Try

trait LocalFSResource {
  implicit val urlResourceTag: ResourceTag[Url[NioPath]] = new ResourceTag[Url[NioPath]] {
    def description: String = "Url[NioPath]"

    // scalafix:off
    def cast(obj: Any): Option[Url[NioPath]] =
      Try(obj.asInstanceOf[Url[NioPath]]).toOption
  }

  implicit val ioFileStoreResourceTag: ResourceTag[Store[IO, NioPath]] = new ResourceTag[Store[IO, NioPath]] {
    def description: String = "Store[IO, NioPath]"

    def cast(obj: Any): Option[Store[IO, NioPath]] = Try(obj.asInstanceOf[Store[IO, NioPath]]).toOption
  }
  // scalafix:on

}

object LocalFSResource extends GlobalResource with LocalFSResource {

  val tmpResource: Resource[IO, Unit] =
    Resource.make(IO.unit)(_ => Files[IO].deleteDirectoryRecursively(Paths.get("tmp")))

  val transferStoreWithRootResource: Resource[IO, (Url[NioPath], Store[IO, NioPath])] =
    Resource.make {
      val p = Paths.get("tmp/transfer-store-root")
      (
        Url(
          "file",
          Authority.localhost,
          Path.of(p.toAbsolutePath.toString, NioPath(p.toAbsolutePath, size = None, isDir = true, lastModified = None))
        ),
        FileStore[IO].lift((u: Url[String]) => u.path.valid)
      ).pure[IO]
    } { case (u, s) =>
      s.remove(u, recursive = true)
    }

  override def sharedResources(global: GlobalWrite): Resource[IO, Unit] =
    for {
      _         <- tmpResource
      (tsr, ts) <- transferStoreWithRootResource
      _         <- global.putR(tsr, Some("transfer-store-root"))
      _         <- global.putR(ts, Some("transfer-store"))
    } yield ()

  // Provides a fallback to support running individual tests via testOnly
  def sharedTransferStoreRootResourceOrFallback(read: GlobalRead): Resource[IO, (Url[NioPath], Store[IO, NioPath])] =
    for {
      maybeTsr <- read.getR[Url[NioPath]](Some("transfer-store-root"))
      maybeTs  <- read.getR[Store[IO, NioPath]](Some("transfer-store"))
      r <- (maybeTsr, maybeTs).tupled match {
        case Some(v) => Resource.eval(IO(v))
        case None    => tmpResource >> transferStoreWithRootResource
      }
    } yield r
}
