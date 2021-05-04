package blobstore

import blobstore.fs.NioPath
import cats.effect.Resource

import java.util.UUID
import fs2.Stream

import blobstore.url.{Authority, FsObject, Path, Url}
import cats.data.Chain
import cats.effect.std.Random
import cats.effect.IO
import cats.syntax.all._
import weaver.{Expectations, GlobalRead, IOSuite, Log}
import weaver.scalacheck._

import scala.concurrent.duration.FiniteDuration

abstract class AbstractStoreTest[B <: FsObject, T](global: GlobalRead)
  extends IOSuite
  with Checkers
  with LocalFSResource {

  override def checkConfig: CheckConfig = super.checkConfig.copy(perPropertyParallelism = 100, minimumSuccessful = 100)

  type Res = TestResource[B, T]

  def scheme: String
  def authority: Authority

  // All test data goes under this path
  def testRunRoot: Path.Plain

  // This path used for testing root level listing. Can be overridden by tests for stores that doesn't allow access
  // to the real root. No writing is done to this path.
  def fileSystemRoot: Path.Plain

  def transferStoreResources: Resource[IO, (Url[NioPath], Store[IO, NioPath])] =
    LocalFSResource.sharedTransferStoreRootResourceOrFallback(global)

  val testRun: UUID = java.util.UUID.randomUUID()

  test("put, list, get, remove keys") { (res, log) =>
    val dir = dirUrl("all")
    for {
      (url, written) <- writeRandomFile(res.store, log)(dir)
      listed1        <- res.store.listAll(url)
      content        <- res.store.get(url, 128).compile.to(Array)
      _              <- res.store.remove(url)
      listed2        <- res.store.listAll(url)
    } yield {
      expect.all(
        listed1.size == 1,
        listed1.headOption.exists(_.toString == url.toString),
        written sameElements content,
        listed2 == Nil
      )
    }

  }

  test("move keys") { (res, log) =>
    val dir = dirUrl("move-keys")

    for {
      (srcUrl, srcContent) <- writeRandomFile(res.store, log)(dir)
      dstUrl               <- randomAlphanumeric(20).map(r => dir / s"$r.bin")
      l1                   <- res.store.listAll(srcUrl)
      l2                   <- res.store.listAll(dstUrl)
      _                    <- res.store.move(srcUrl, dstUrl)
      l3                   <- res.store.listAll(srcUrl)
      l4                   <- res.store.listAll(dstUrl)
      content              <- res.store.get(dstUrl, 128).compile.to(Array)
    } yield {
      expect.all(l1.nonEmpty, l2.isEmpty, l3.isEmpty, l4.nonEmpty, srcContent sameElements content)

    }

  }

  test("list multiple keys") { (res, log) =>
    val dir = dirUrl("list-many")
    for {
      urls <- (1 to 10).toList.traverse(_ => writeRandomFile(res.store, log)(dir))
      l    <- res.store.listAll(dir)
    } yield {
      expect(urls.map(_._1.show).toSet == l.map(_.show).toSet)
    }

  }

  // We've had some bugs involving directories at the root level, since it is a bit of an edge case.
  // Worth noting that that most of these tests operate on files that are in nested directories, avoiding
  // any problems that there might be with operating on a root level file/directory.
  test("list files in a root level directory") { (res, log) =>
    val fsRootUrl = Url(scheme, authority, fileSystemRoot)
    val r = Resource.make {
      (1 to 2).toList.traverse(_ => writeRandomFile(res.store, log)(fsRootUrl))
    } { urls =>
      urls.traverse_ { case (u, _) => res.store.remove(u) }

    }

    r.use { urls =>
      res.store.listAll(fsRootUrl).map { l =>
        // Not doing equals comparison because this directory contains files from other tests.
        // Also, some stores will prepend a "/" before the filenames. Doing a string comparison to ignore this detail for now.
        val listedString = l.map(_.show).mkString(", ")
        urls.map { case (u, _) => expect(listedString.contains(u.path.relative.show)) }.combineAll
      }

    }

  }

  test("stat urls") { (res, log) =>
    val dir = dirUrl("list-dirs")
    for {
      _ <-
        List("subdir/file-1.txt", "file-2.txt").traverse(suffix => writeRandomFile(res.store, log)(dir, Some(suffix)))
      s <- res.store.stat(dir / "subdir" / "file-1.txt").compile.lastOrError
    } yield {
      expect(s.path.fileName.contains("file-1.txt"))
    }
  }

  test("list files and directories correctly") { (res, log) =>
    val dir = dirUrl("list-dirs")
    for {
      urls <-
        List("subdir/file-1.txt", "file-2.txt").traverse(suffix => writeRandomFile(res.store, log)(dir, Some(suffix)))
      l <- res.store.listAll(dir)
      _ <- l.traverse_(u => log.debug(u.toString))

    } yield {
      expect.all(
        l.map(_.show.stripSuffix("/")).toSet == urls.map(_._1.show.replaceFirst("/file-1.txt", "")).toSet,
        l.find(_.path.isDir).flatMap(_.path.lastSegment).map(_.stripSuffix("/")).contains("subdir")
      )
    }

  }

  test("transfer individual file to a file path from one store to another") { (res, log) =>
    val srcDir = dirUrl("transfer-file-to-file")
    transferStoreResources.use { case (transferStoreRoot, transferStore) =>
      for {
        (srcUrl, srcContent) <- writeRandomFile(res.store, log)(srcDir)
        dstDir = transferStoreRoot / testRun.toString / srcDir.path.lastSegment
        dstUrl = dstDir / srcUrl.path.lastSegment
        _           <- log.debug(s"Transfer from $srcUrl to $dstUrl")
        transferred <- res.store.transferTo(transferStore, srcUrl, dstUrl)
        dstContent  <- transferStore.get(dstUrl, 128).compile.to(Array)
      } yield {
        expect.all(
          transferred == 1,
          srcContent sameElements dstContent
        )
      }

    }

  }

  test("transfer directories recursively from one store to another") { (res, log) =>
    val srcDir = dirUrl("transfer-dir-rec")

    transferStoreResources.use { case (transferStoreRoot, transferStore) =>
      for {
        srcUrls <- (1 to 10).toList.traverse(i =>
          writeRandomFile(res.store, log)(srcDir, Some(if (i % 2 == 0) s"$i.bin" else s"subdir/$i.bin"))
        )
        srcContents = srcUrls.foldLeft(Map.empty[String, List[Byte]]) { case (acc, (u, bytes)) =>
          u.path.lastSegment.fold(acc)(acc.updated(_, bytes.toList))
        }
        dstDir = transferStoreRoot / testRun.toString / srcDir.path.lastSegment
        _           <- log.debug(s"Transfer from dir $srcDir to dir $dstDir")
        transferred <- res.store.transferTo(transferStore, srcDir, dstDir)
        dstContents <- transferStore.list(dstDir, recursive = true).evalMap { u =>
          transferStore.get(u, 128).compile.to(List).map(u.path.lastSegment -> _)
        }.compile.fold(Map.empty[String, List[Byte]]) { case (acc, (k, v)) => k.fold(acc)(acc.updated(_, v)) }
      } yield {
        expect.all(
          transferred == 10,
          srcContents == dstContents
        )
      }

    }

  }

  test("copy file within a store") { (res, log) =>
    val srcDir = dirUrl("copy-dir-to-dir/src")
    val dstDir = dirUrl("copy-dir-to-dir/dst")
    for {
      (srcUrl, srcOriginalContent) <- writeRandomFile(res.store, log)(srcDir)
      dstUrl = dstDir / srcUrl.path.lastSegment
      _          <- res.store.copy(srcUrl, dstUrl)
      srcContent <- res.store.get(srcUrl, 128).compile.to(Array)
      dstContent <- res.store.get(dstUrl, 128).compile.to(Array)

    } yield {
      expect.all(
        srcOriginalContent sameElements srcContent,
        srcContent sameElements dstContent
      )
    }

  }

  test("remove files and directories recursively") { (res, log) =>
    val dir = dirUrl("rm-dir")
    for {
      _ <- (1 to 10).toList.traverse { i =>
        writeRandomFile(res.store, log)(dir, Some(if (i % 2 == 0) s"$i.bin" else s"subdir/$i.bin"))
      }
      l1 <- res.store.list(dir, recursive = true).compile.toList
      _  <- res.store.remove(dir, recursive = true)
      l2 <- res.store.listAll(dir)
    } yield {
      expect.all(
        l1.size == 10,
        l2.isEmpty
      )
    }
  }

  test("fail stream when getting non-existing file") { res =>
    val url = dirUrl("get-nonexistent") / "no-file"
    res.store.get(url, 128).compile.to(Array).attempt.map { result =>
      expect(result.isLeft)
    }
  }

  test("succeed on remove when path does not exist") { res =>
    val url = dirUrl("rm-nonexistent") / "no-file"
    res.store.remove(url).attempt.map { result =>
      expect(result.isRight)
    }

  }

  test("put content with unknown size") { res =>
    val url = dirUrl("put-unknown-size") / "file.bin"

    for {
      data    <- randomBytes(25)
      _       <- Stream.emits(data).through(res.store.put(url)).compile.drain
      content <- res.store.get(url, 128).compile.to(Array)
    } yield {
      expect(data sameElements content)
    }
  }

  test("overwrite existing file on put with overwrite") { (res, log) =>
    val dir = dirUrl("overwrite-existing")
    for {
      (url, original) <- writeRandomFile(res.store, log)(dir)
      originalContent <- res.store.get(url, 128).compile.to(Array)
      newContent      <- randomBytes(25)
      _               <- Stream.emits(newContent).through(res.store.put(url)).compile.drain
      content         <- res.store.get(url, 128).compile.to(Array)
    } yield {
      expect.all(
        original sameElements originalContent,
        newContent sameElements content
      )
    }

  }

  test("fail on put to url with existing file without overwrite") { (res, log) =>
    val dir = dirUrl("fail-no-overwrite")
    for {
      (url, original) <- writeRandomFile(res.store, log)(dir)
      newContent      <- randomBytes(25)
      result          <- Stream.emits(newContent).through(res.store.put(url, overwrite = false)).compile.drain.attempt
      content         <- res.store.get(url, 128).compile.to(Array)
    } yield {
      expect.all(
        result.isLeft,
        content sameElements original
      )
    }
  }

  test("put to new Path without overwrite") { res =>
    val url = dirUrl("no-overwrite") / "file.bin"
    for {
      data    <- randomBytes(25)
      _       <- Stream.emits(data).through(res.store.put(url, overwrite = false)).compile.drain.attempt
      content <- res.store.get(url, 128).compile.to(Array)
    } yield {
      expect(data sameElements content)
    }

  }

  test("support paths with spaces") { (res, log) =>
    val dir = dirUrl("path spaces")
    def last2Segments(url: Url[B]): Chain[String] =
      Chain.fromSeq(url.path.segments.reverseIterator.take(2).toSeq.reverse)

    val expected = Chain("path spaces", "file with spaces")

    for {
      (url, original) <- writeRandomFile(res.store, log)(dir, Some("file with spaces"))
      l1              <- res.store.listAll(url)
      l2              <- res.store.listAll(dir)
      content         <- res.store.get(url, 128).compile.to(Array)
      _               <- res.store.remove(url)
      l3              <- res.store.listAll(url)
    } yield {
      expect.all(
        original sameElements content,
        l1.size == 1,
        l2.size == 1,
        l3.isEmpty,
        last2Segments(l1.head) == expected,
        last2Segments(l2.head) == expected
      )
    }

  }

  test("list recursively") { (res, log) =>
    val dir = dirUrl("list-recursively")
    for {
      _ <- List("a", "b", "c", "sub-folder/d", "sub-folder/sub-sub-folder/e", "x", "y", "z").traverse { suffix =>
        writeRandomFile(res.store, log)(dir, Some(suffix))

      }
      l <- res.store.listAll(dir, recursive = true)
    } yield {
      expect(l.size == 8) and l.map { u => expect(!u.path.isDir) }.combineAll
    }

  }

  test("put data while rotating files") { (res, log) =>
    val dir = dirUrl("put-rotating")

    for {
      _       <- writeRandomFile(res.store, log)(dir, Some("3"))
      data    <- randomBytes(110)
      counter <- IO.ref(0)
      _ <- Stream.emits(data)
        .through(res.store.putRotate(counter.getAndUpdate(_ + 1).map(dir / _.toString), 20L))
        .compile.drain
      l <- res.store.listAll(dir, recursive = true)
      contents <- l.flatTraverse { u =>
        res.store.get(u, 128).compile.to(Array).map(c => u.path.lastSegment.map(_ -> c).toList)
      }.map(_.toMap)
    } yield {
      (0 to 5).toList.map { i =>
        expect(contents.get(i.toString).exists(_ sameElements data.slice(i * 20, (i + 1) * 20)))
      }.combineAll

    }

  }

  test("put empty (zero-byte) files") { res =>
    val file = dirUrl("empty-file") / "file"

    for {
      result  <- Stream.empty.through(res.store.put(file)).compile.drain.attempt
      content <- res.store.get(file, 128).compile.toList
    } yield {
      expect.all(
        result.isRight,
        content == Nil
      )
    }

  }

  test("read same data that was written") { res =>
    val dir = dirUrl("read-write")
    forall[List[Byte], IO[Expectations]] { bytes: List[Byte] =>
      val blob = Stream.emits(bytes)
      for {
        filePath <- randomAlphanumeric(20).map(dir / _)
        _        <- blob.through(res.store.put(filePath)).compile.drain.attempt
        contents <- res.store.get(filePath, 1024).compile.to(Array)
      } yield {
        expect(contents sameElements bytes)
      }

    }
  }

  def randomAlphanumeric(size: Int): IO[String] = for {
    r <- Random.scalaUtilRandom
    s <- List.fill(size)(r.nextAlphaNumeric).sequence.map(_.mkString)
  } yield s

  def randomBytes(size: Int): IO[Array[Byte]] = Random.scalaUtilRandom.flatMap(_.nextBytes(size))

  def dirUrl(name: String): Url[String] = Url(scheme, authority, testRunRoot `//` name)

  def writeRandomFile[BB <: FsObject](store: Store[IO, BB], log: Log[IO])(
    tmpDir: Url[String],
    filename: Option[String] = None
  ): IO[(Url[String], Array[Byte])] = {
    def retry[AA](process: IO[AA], count: Int, times: Int): IO[AA] = process.handleErrorWith {
      case _ if count <= times =>
        for {
          _      <- log.info(s"Process failed. Sleeping for 500ms and retry ($count/$times)")
          _      <- IO.sleep(FiniteDuration(500, "millis"))
          result <- retry(process, count + 1, times)
        } yield result
      case t =>
        for {
          _      <- log.error(s"Process failed $times times. Giving up...")
          result <- IO.raiseError(t)
        } yield result
    }

    val writeContents = for {
      url     <- filename.fold(randomAlphanumeric(20).map(_ ++ ".bin"))(_.pure).map(tmpDir / _)
      content <- randomBytes(25)
      _       <- Stream.emits(content).through(store.put(url)).compile.drain
    } yield (url, content)

    retry(writeContents, 1, 5)
  }
}
