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

import java.util.UUID
import java.nio.file.{Path => NioPath}
import java.util.concurrent.Executors

import blobstore.fs.FileStore
import blobstore.url.{Authority, FileSystemObject, Path, Url}
import cats.effect.concurrent.Ref
import org.scalatest.{BeforeAndAfterAll, Inside}
import cats.effect.{Blocker, ContextShift, IO, Timer}
import cats.effect.laws.util.TestInstances
import cats.implicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import fs2.Stream

import scala.concurrent.ExecutionContext
import scala.util.Random
import scala.concurrent.duration._

abstract class AbstractStoreTest[A <: Authority, B: FileSystemObject]
  extends AnyFlatSpec
  with Matchers
  with BeforeAndAfterAll
  with TestInstances
  with Inside {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]     = IO.timer(ExecutionContext.global)
  val blocker: Blocker              = Blocker.liftExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))

  val testRun: UUID = java.util.UUID.randomUUID()

  val transferStoreRootDir: Path.Plain = Path(s"tmp/transfer-store-root/$testRun")
  val transferStore: FileStore[IO]     = new FileStore[IO](blocker)

  val store: Store[IO, A, B]
  val scheme: String
  val authority: A

  // This path used for testing root level listing. Can be overridden by tests for stores that doesn't allow access
  // to the real root. No writing is done to this path.
  val fileSystemRoot: Path.Plain = Path("/")

  // All testdata goes under this path
  lazy val testRunRoot: Path.Plain = Path(s"test-$testRun")

  behavior of "all stores"

  it should "put, list, get, remove keys" in {
    val dir = dirPath("all")

    // put a random file
    val filename = s"test-${System.currentTimeMillis}.txt"
    val url      = writeFile(store, dir)(filename)

    // list to make sure file is present
    val found = store.listAll(url).unsafeRunSync()
    found.size must be(1)
    found.head.toString mustBe url.path.toString

    // check contents of file
    store.getContents(url).unsafeRunSync() must be(contents(filename))

    // check remove works
    store.remove(url).unsafeRunSync()
    val notFound = store.listAll(url).unsafeRunSync()
    notFound mustBe Nil
  }

  it should "move keys" in {
    val dir: Url[A] = dirUrl("move-keys")
    val src         = writeFile(store, dir.path)(s"src/${System.currentTimeMillis}.txt")
    val dst         = dir / s"dst/${System.currentTimeMillis}.txt"

    val test = for {
      l1 <- store.listAll(src)
      l2 <- store.listAll(dst)
      _  <- store.move(src, dst)
      l3 <- store.listAll(src)
      l4 <- store.listAll(dst)
      _  <- store.remove(dst, recursive = false)
    } yield {
      l1.isEmpty must be(false)
      l2.isEmpty must be(true)
      l3.isEmpty must be(true)
      l4.isEmpty must be(false)
    }

    test.unsafeRunSync()

  }

  it should "list multiple keys" in {
    import cats.implicits._

    val dir: Url[A] = dirUrl("list-many")

    val paths = (1 to 10).toList
      .map(i => s"filename-$i.txt")
      .map(writeFile(store, dir.path))

    val exp = paths.map(_.path.show).toSet

    store.listAll(dir).unsafeRunSync().map(_.show).toSet must be(exp)

    paths.parTraverse_(store.remove(_, recursive = false)).unsafeRunSync()

    store.listAll(dir).unsafeRunSync() mustBe Nil
  }

  // We've had some bugs involving directories at the root level, since it is a bit of an edge case.
  // Worth noting that that most of these tests operate on files that are in nested directories, avoiding
  // any problems that there might be with operating on a root level file/directory.
  it should "listAll lists files in a root level directory" in {
    import cats.implicits._
    val rootDir = fileSystemRoot
    val paths = (1 to 2).toList
      .map(i => s"filename-$i-$testRun.txt")
      .map(writeFile(store, rootDir))

    val exp = paths.map(p => p.path.relative.show).toSet

    // Not doing equals comparison because this directory contains files from other tests.
    // Also, some stores will prepend a "/" before the filenames. Doing a string comparison to ignore this detail for now.
    val pathsListed =
      store.listAll(Url(scheme, authority, rootDir)).unsafeRunSync().map(_.show).toSet.toString()
    exp.foreach(s => pathsListed must include(s))

    val io: IO[List[Unit]] = paths.traverse(store.remove(_))
    io.unsafeRunSync()
  }

  it should "list files and directories correctly" in {
    val dir: Url[A] = dirUrl("list-dirs")
    val paths       = List("subdir/file-1.txt", "file-2.txt").map(writeFile(store, dir.path))
    val exp         = paths.map(_.path.show.replaceFirst("/file-1.txt", "")).toSet

    val ls = store.listAll(dir).unsafeRunSync()
    ls.map(_.show.stripSuffix("/")).toSet must be(exp)
    val option    = ls.find(_.isDir)
    val dirString = option.flatMap(_.lastSegment)
    inside(dirString) {
      case Some(dir) => dir must fullyMatch regex "subdir/?"
    }

    val io: IO[List[Unit]] = paths.parTraverse(store.remove(_))
    io.unsafeRunSync()
  }

  it should "transfer individual file to a directory from one store to another" in {
    val srcPath =
      writeLocalFile(transferStore, localDirPath("transfer-single-file-to-dir-src"))("transfer-filename.txt")

    val dstDir  = dirUrl("transfer-single-file-to-dir-dst")
    val dstPath = dstDir / srcPath.lastSegment

    val test = for {
      i <- transferStore.transferTo(store, srcPath, dstDir)
      c1 <- transferStore
        .getContents(srcPath)
        .handleError(e => s"FAILED transferStore.getContents $srcPath: ${e.getMessage}")
      c2 <- store.getContents(dstPath).handleError(e => s"FAILED store.getContents $dstPath: ${e.getMessage}")
      _  <- transferStore.remove(srcPath, recursive = false).handleError(_ => ())
      _  <- store.remove(dstPath, recursive = false).handleError(_ => ())
    } yield {
      i must be(1)
      c1 must be(c2)
    }

    test.unsafeRunSync()
  }

  it should "transfer individual file to a file path from one store to another" in {
    val srcPath = writeLocalFile(transferStore, localDirPath("transfer-file-to-file-src"))("src-filename.txt")

    val dstPath = dirUrl("transfer-file-to-file-dst") / "dst-file-name.txt"

    val test = for {
      i <- transferStore.transferTo(store, srcPath, dstPath)
      c1 <- transferStore
        .getContents(srcPath)
        .handleError(e => s"FAILED transferStore.getContents $srcPath: ${e.getMessage}")
      c2 <- store
        .getContents(dstPath)
        .handleError(e => s"FAILED store.getContents $dstPath: ${e.getMessage}")
      _ <- transferStore.remove(srcPath, recursive = false).handleError(_ => ())
      _ <- store.remove(dstPath, recursive = false).handleError(_ => ())
    } yield {
      i must be(1)
      c1 must be(c2)
    }

    test.unsafeRunSync()
  }

  it should "transfer directory to a directory path from one store to another" in {
    val srcDir = localDirPath("transfer-dir-to-dir-src")
    val dstDir = dirUrl("transfer-dir-to-dir-dst")

    val paths = (1 until 10).toList
      .map(i => s"filename-$i.txt")
      .map(writeLocalFile(transferStore, srcDir))

    val test = for {
      i <- transferStore.transferTo(store, srcDir, dstDir)
      c1 <- paths.traverse { p =>
        transferStore
          .getContents(p)
          .handleError(e => s"FAILED transferStore.getContents $p: ${e.getMessage}")
      }
      c2 <- paths.traverse { p =>
        store
          .getContents(dstDir / p.lastSegment)
          .handleError(e => s"FAILED store.getContents ${dstDir / p.lastSegment}: ${e.getMessage}")
      }
      _ <- paths.traverse(transferStore.remove(_, recursive = false).handleError(_ => ()))
      _ <- paths.traverse(p => store.remove(dstDir / p.lastSegment, recursive = false).handleError(_ => ()))
    } yield {
      i must be(paths.length)
      c2 must be(c1)
    }

    test.unsafeRunSync()
  }

  it should "transfer directories recursively from one store to another" in {
    val srcDir = localDirPath("transfer-dir-rec-src")
    val dstDir = dirUrl("transfer-dir-rec-dst")

    val paths1 = (1 to 5).toList
      .map(i => s"filename-$i.txt")
      .map(writeLocalFile(transferStore, srcDir))

    val paths2 = (6 until 10).toList
      .map(i => s"subdir/filename-$i.txt")
      .map(writeLocalFile(transferStore, srcDir))

    val paths = paths1 ++ paths2

    val test = for {
      i <- transferStore.transferTo(store, srcDir, dstDir)
      c1 <- paths.traverse { p =>
        transferStore.getContents(p).handleError(e => s"FAILED transferStore.getContents $p: ${e.getMessage}")
      }
      c2 <- {
        paths1.map { p =>
          store
            .getContents(dstDir / p.lastSegment)
            .handleError(e => s"FAILED store.getContents ${dstDir / p.lastSegment}: ${e.getMessage}")
        } ++
          paths2.map { p =>
            store
              .getContents(dstDir / "subdir" / p.lastSegment)
              .handleError(e => s"FAILED store.getContents ${dstDir / "subdir" / p.lastSegment}: ${e.getMessage}")
          }
      }.sequence
      _ <- paths.traverse(transferStore.remove(_, recursive = false).handleError(_ => ()))
      _ <- paths1.traverse(p => store.remove(dstDir / p.lastSegment, recursive = false).handleError(_ => ()))
      _ <- paths2.traverse(p => store.remove(dstDir / "subdir" / p.lastSegment, recursive = false).handleError(_ => ()))
    } yield {
      i must be(9)
      c1.mkString("\n") must be(c2.mkString("\n"))
    }

    test.unsafeRunSync()
  }

  it should "copy files in a store from one directory to another" in {
    val srcDir = dirUrl("copy-dir-to-dir-src")
    val dstDir = dirUrl("copy-dir-to-dir-dst")

    writeFile(store, srcDir.path)("filename.txt")

    val test = for {
      _ <- store.copy(srcDir / "filename.txt", dstDir / "filename.txt")
      c1 <- store
        .getContents(srcDir / "filename.txt")
        .handleError(e => s"FAILED getContents: ${e.getMessage}")
      c2 <- store
        .getContents(dstDir / "filename.txt")
        .handleError(e => s"FAILED getContents: ${e.getMessage}")
      _ <- store.remove(dstDir / "filename.txt")
      _ <- store.remove(srcDir / "filename.txt")
    } yield {
      c1 mustBe c2
    }

    test.unsafeRunSync()
  }

  it should "remove files and directories recursively" in {
    val srcDir = dirUrl("rm-dir-to-dir-src")

    (1 to 10).toList
      .map(i => s"filename-$i.txt")
      .map(writeFile(store, srcDir.path))

    (1 to 5).map(i => s"filename-$i.txt").map(writeFile(store, (srcDir / "sub").path))

    store.remove(srcDir, recursive = true).unsafeRunSync()

    store.list(srcDir).compile.toList.unsafeRunSync() mustBe Nil
  }

  it should "succeed on remove when path does not exist" in {
    val dir  = dirUrl("remove-nonexistent-path")
    val path = dir / "no-file.txt"
    store.remove(path).unsafeRunSync()
  }

  it should "support putting content with no size" in {
    val dir: Url[A] = dirUrl("put-no-size")
    val path        = dir / "no-size.txt"
    val exp         = contents("put without size")
    val test = for {
      _ <- fs2
        .Stream(exp)
        .covary[IO]
        .through(fs2.text.utf8Encode)
        .through(store.put(path))
        .compile.drain
      res <- store.getContents(path)
      _   <- store.remove(path, recursive = false)
    } yield res must be(exp)

    test.unsafeRunSync()
  }

  it should "return failed stream when getting non-existing file" in {
    val test = for {
      res <- store.get(dirUrl("foo") / "doesnt-exists.txt", 4096).attempt.compile.lastOrError
    } yield res mustBe a[Left[_, _]]

    test.unsafeRunSync()
  }

  it should "overwrite existing file on put with overwrite" in {
    val dir  = dirUrl("overwrite-existing")
    val path = writeFile(store, dir.path)("existing.txt")

    fs2
      .Stream("new content".getBytes().toIndexedSeq: _*)
      .through(store.put(path))
      .compile
      .drain
      .unsafeRunSync()

    val content = store
      .get(path, 1024)
      .compile
      .to(Array)
      .map(bytes => new String(bytes))
      .unsafeRunSync()

    content mustBe "new content"
  }

  it should "fail on put to Path with existing file without overwrite" in {
    val dir  = dirUrl("fail-no-overwrite")
    val path = writeFile(store, dir.path)("existing.txt")

    val result = fs2
      .Stream("new content".getBytes().toIndexedSeq: _*)
      .through(store.put(path, overwrite = false))
      .compile
      .drain
      .attempt
      .unsafeRunSync()

    result mustBe a[Left[_, _]]
  }

  it should "put to new Path without overwrite" in {
    val dir  = dirUrl("no-overwrite")
    val path = dir / "new.txt"

    fs2
      .Stream("new content".getBytes().toIndexedSeq: _*)
      .through(store.put(path, overwrite = false))
      .compile
      .drain
      .attempt
      .unsafeRunSync()

    val content = store
      .get(path, 1024)
      .compile
      .to(Array)
      .map(bytes => new String(bytes))
      .unsafeRunSync()

    content mustBe "new content"
  }

  it should "support paths with spaces" in {
    val dir = dirUrl("path spaces")
    val url = writeFile(store, dir.path)("file with spaces")
    val result = for {
      list <- store
        .list(url)
        .compile
        .toList
      get    <- store.get(url, 1024).compile.drain.attempt
      remove <- store.remove(url, recursive = false).attempt
    } yield {
      list.map(_.show) must contain only url.path.show
      list.headOption.flatMap(_.fileName) must contain("file with spaces")
      list.headOption.map(_.segments.toList.init.last) must contain("path spaces")
      get mustBe a[Right[_, _]]
      remove mustBe a[Right[_, _]]
    }
    result.unsafeRunSync()
  }

  it should "be able to list recursively" in {
    val dir   = dirUrl("list-recursively")
    val files = List("a", "b", "c", "sub-folder/d", "sub-folder/sub-sub-folder/e", "x", "y", "z").map(dir / _)
    val result = for {
      _     <- files.traverse(p => Stream.emit(0: Byte).through(store.put(p)).compile.drain)
      paths <- store.list(dir, recursive = true).compile.toList
    } yield {
      paths must have size 8
      paths.flatMap(_.fileName) must contain theSameElementsAs List("a", "b", "c", "d", "e", "x", "y", "z")
      paths.foreach { p =>
        p.isDir mustBe false
        p.size must contain(1L)
      }
    }
    result.unsafeRunSync()
  }

  it should "put data while rotating files" in {
    val fileCount      = 5L
    val fileLength     = 20
    val lastFileLength = 10
    val dir            = dirUrl("put-rotating")
    val bytes          = randomBA(fileLength)
    val lastFileBytes  = randomBA(lastFileLength)
    val data = Stream.emit(bytes).repeat.take(fileCount).flatMap(bs => Stream.emits(bs.toIndexedSeq)) ++
      Stream.emits(lastFileBytes.toIndexedSeq)

    // Check overwrite
    writeFile(store, dir.path)("3")

    val test = for {
      counter <- Ref.of[IO, Int](0)
      _ <- data
        .through(store.putRotate(counter.getAndUpdate(_ + 1).map(i => dir / s"$i"), fileLength.toLong))
        .compile
        .drain
      files        <- store.list(dir).compile.toList
      fileContents <- files.traverse(p => store.get(dir.replacePath(p), fileLength).compile.to(Array).map(p -> _))
    } yield {
      files must have size (fileCount + 1)
      files.flatMap(_.fileName) must contain allElementsOf (0L to fileCount).map(_.toString)
      files.foreach { p =>
        p.isDir mustBe false
        p.size must contain(if (p.fileName.contains(fileCount.toString)) lastFileLength else fileLength)
      }
      fileContents.foreach {
        case (p, content) =>
          content mustBe (if (p.fileName.contains(fileCount.toString)) lastFileBytes else bytes)
      }

    }
    test.unsafeRunSync()
  }

  def dirUrl(name: String): Url[A] = Url(scheme, authority, testRunRoot `//` name)

  def dirPath(name: String): Path.Plain = testRunRoot `//` name

  def localDirPath(name: String): Path.Plain = transferStoreRootDir / name

  def contents(filename: String): String = s"file contents to upload: $filename"

  def writeFile(store: Store[IO, A, B], tmpDir: Path.Plain)(filename: String): Url[A] = {
    def retry[AA](io: IO[AA], count: Int, times: Int): IO[AA] = io.handleErrorWith { t =>
      if (count < times) Timer[IO].sleep(500.millis) >> retry(io, count + 1, times) else IO.raiseError(t)
    }
    val url = Url(scheme, authority, tmpDir / filename)

    val writeContents = store.put(contents(filename), url).compile.drain.as(url)
    retry(writeContents, 0, 5).unsafeRunSync()
  }

  def writeLocalFile(store: FileStore[IO], tmpDir: Path.Plain)(filename: String): Path.Plain = {
    val url = tmpDir / filename
    store.put(contents(filename), url, overwrite = true).compile.drain.unsafeRunSync()
    url
  }

  def randomBA(length: Int): Array[Byte] = {
    val ba = new Array[Byte](length)
    Random.nextBytes(ba)
    ba
  }

  // remove dirs created by AbstractStoreTest
  override def afterAll(): Unit = cleanup(transferStoreRootDir.nioPath.resolve(testRunRoot.nioPath))

  def cleanup(root: NioPath): Unit = {

    import java.io.IOException
    import java.nio.file.{FileVisitor, FileVisitResult, Files, SimpleFileVisitor, Path => NioPath}
    import java.nio.file.attribute.BasicFileAttributes

    val fv: FileVisitor[NioPath] = new SimpleFileVisitor[NioPath]() {
      override def postVisitDirectory(dir: NioPath, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }

      override def visitFile(file: NioPath, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }
    }

    try { Files.walkFileTree(root, fv); () }
    catch { case _: Throwable => }
  }

}
