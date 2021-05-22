package blobstore
package sftp

import blobstore.url.{Authority, Host, Path, Port}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import com.dimafeng.testcontainers.GenericContainer
import com.jcraft.jsch.Session

import scala.concurrent.duration.FiniteDuration

abstract class AbstractSftpStoreTest extends AbstractStoreTest[SftpFile, SftpStore[IO]] {

  def container: GenericContainer

  override val scheme = "sftp"

  override def authority: Authority =
    Authority(Host.unsafe(container.containerIpAddress), None, Some(Port(container.mappedPort(22))))

  override val fileSystemRoot: Path.Plain = Path("sftp_tests")
  override val testRunRoot: Path.Plain    = Path(s"sftp_tests/$testRun")

  def sessionResource: Resource[IO, Session]

  def cleanup(store: SftpStore[F]): F[Unit] = store.remove(testRunRoot, recursive = true)

  override def sharedResource: Resource[IO, TestResource[SftpFile, SftpStore[IO]]] = {
    for {
      (tsr, ts) <- transferStoreResources
      session   <- sessionResource
      store     <- SftpStore(session.pure, Some(10), 50000)
      _         <- Resource.onFinalize(cleanup(store))
    } yield TestResource(store.lift, store, FiniteDuration(8, "s"), tsr, ts)
  }

  test("list files in current working directory") { res =>
    val empty = Path("")
    val dot   = Path(".")
    val test = for {
      emptyList <- res.extra.list(empty).compile.toList
      dotList   <- res.extra.list(dot).compile.toList
    } yield {
      expect.all(
        emptyList.flatMap(_.lastSegment) == List("sftp_tests/"),
        dotList.flatMap(_.lastSegment) == List("sftp_tests/")
      )
    }

    test.timeout(res.timeout)
  }

  test("list more than 64 (default queue/buffer size) keys") { (res, log) =>
    val dir = dirUrl("list-more-than-64")
    val test = for {
      urls <- (1 to 256).toList.traverse(_ => writeRandomFile(res.store, log)(dir))
      l    <- res.store.listAll(dir)
    } yield {
      expect.all(
        urls.map(_._1.path.lastSegment).toSet == l.map(_.path.lastSegment).toSet
      )
    }

    test.timeout(res.timeout)

  }

  test("remove empty directory") { (res, log) =>
    val dir = dirUrl("remove-empty-dir")
    val test = for {
      (url, _) <- writeRandomFile(res.store, log)(dir)
      _        <- res.store.remove(url)
      _        <- res.store.remove(dir)
      l        <- res.store.listAll(dir)
    } yield {
      expect(l.isEmpty)
    }

    test.timeout(res.timeout)

  }

  test("don't remove non-empty directory") { (res, log) =>
    val dir = dirUrl("dont-remove-non-empty")
    val test = for {
      _      <- writeRandomFile(res.store, log)(dir)
      result <- res.store.remove(dir).attempt
      l      <- res.store.listAll(dir)
    } yield {
      expect.all(result.isLeft, l.size == 1)
    }

    test.timeout(res.timeout)

  }

}
