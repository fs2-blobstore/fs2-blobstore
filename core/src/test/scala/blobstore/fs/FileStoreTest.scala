package blobstore
package fs

import blobstore.url.{Authority, Path, Url}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import java.nio.file.Paths
import scala.concurrent.duration.FiniteDuration

object FileStoreTest extends AbstractStoreTest[NioPath, Unit] {

  override val scheme: String       = "file"
  override val authority: Authority = Authority.localhost

  override val testRunRoot: Path.Plain    = Path(Paths.get(s"tmp/filestore/$testRun/").toAbsolutePath.toString)
  override val fileSystemRoot: Path.Plain = testRunRoot.parentPath

  override val sharedResource: Resource[IO, TestResource[NioPath, Unit]] =
    for {
      fs        <- Resource.make(FileStore[IO].pure)(fs => fs.remove(testRunRoot, recursive = true))
      (tsr, ts) <- transferStoreResources
    } yield TestResource(fs.lift((u: Url[String]) => u.path.valid), (), FiniteDuration(1, "s"), tsr, ts)

  test("no side effects when creating a Pipe") { res =>
    val dir = dirUrl("should-not") / "be" `//` "created"

    res.store.put(dir / "file.txt")

    res.store.list(dir.withPath(dir.path.parentPath)).compile.toList.map(list => expect(list.isEmpty))

  }
}
