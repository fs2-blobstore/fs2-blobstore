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
import java.nio.file.{Paths, Path => NioPath}
import java.util.concurrent.Executors

import blobstore.Store.BlobStore
import blobstore.url.Authority.Bucket
import blobstore.url.{Path, Url}
import cats.effect.concurrent.Ref
import org.scalatest.BeforeAndAfterAll
import cats.effect.{Blocker, ContextShift, IO}
import cats.effect.laws.util.TestInstances
import cats.implicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import fs2.Stream

import scala.concurrent.ExecutionContext
import scala.util.Random

trait AbstractStoreTest[B] extends AnyFlatSpec with Matchers with BeforeAndAfterAll with TestInstances {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  val blocker: Blocker              = Blocker.liftExecutionContext(ExecutionContext.fromExecutor(Executors.newCachedThreadPool))

//  val transferStoreRootDir: NioPath = Paths.get("tmp/transfer-store-root/")
//  val transferStore: BlobStore[IO, B]      = new LocalStore[IO](transferStoreRootDir, blocker)

  val store: BlobStore[IO, B]
  val scheme: String
  val root: Bucket

  val testRun: UUID            = java.util.UUID.randomUUID()
  lazy val testRunRoot: Path.Plain = Path(s"/test-$testRun")

  behavior of "all stores"
  it should "put, list, get, remove keys" in {
    val dir: Url.Bucket = dirPath("all")

    // put a random file
    val filename = s"test-${System.currentTimeMillis}.txt"
    val url     = writeFile(store, dir.path)(filename)

    // list to make sure file is present
    val found = store.list(url).compile.toList.unsafeRunSync()
    found.size must be(1)
    found.head.toString must be(url.path.toString)

    // check contents of file
    store.get(url, 4096).through(fs2.text.utf8Decode).compile.string.unsafeRunSync() must be(contents(filename))

    // check remove works
    store.remove(url).unsafeRunSync()
    val notFound = store.list(url).compile.toList.unsafeRunSync()
    notFound.isEmpty must be(true)
  }
//
//  it should "move keys" in {
//    val dir: Path = dirPath("move-keys")
//    val src       = writeFile(store, dir)(s"src/${System.currentTimeMillis}.txt")
//    val dst       = dir / s"dst/${System.currentTimeMillis}.txt"
//
//    val test = for {
//      l1 <- store.listAll(src)
//      l2 <- store.listAll(dst)
//      _  <- store.move(src, dst)
//      l3 <- store.listAll(src)
//      l4 <- store.listAll(dst)
//      _  <- store.remove(dst)
//    } yield {
//      l1.isEmpty must be(false)
//      l2.isEmpty must be(true)
//      l3.isEmpty must be(true)
//      l4.isEmpty must be(false)
//    }
//
//    test.unsafeRunSync()
//
//  }
//
//  it should "list multiple keys" in {
//    import cats.implicits._
//
//    val dir: Path = dirPath("list-many")
//
//    val paths = (1 to 10).toList
//      .map(i => s"filename-$i.txt")
//      .map(writeFile(store, dir))
//
//    val exp = paths.map(_.filePath).toSet
//
//    store.listAll(dir).unsafeRunSync().map(_.filePath).toSet must be(exp)
//
//    val io: IO[List[Unit]] = paths.map(store.remove).sequence
//    io.unsafeRunSync()
//
//    store.listAll(dir).unsafeRunSync().isEmpty must be(true)
//  }
//
//  // We've had some bugs involving directories at the root level, since it is a bit of an edge case.
//  // Worth noting that that most of these tests operate on files that are in nested directories, avoiding
//  // any problems that there might be with operating on a root level file/directory.
//  it should "listAll lists files in a root level directory" in {
//    import cats.implicits._
//    val rootDir = Path(root)
//    val paths = (1 to 2).toList
//      .map(i => s"filename-$i-$testRun.txt")
//      .map(writeFile(store, rootDir))
//
//    val exp = paths.map(p => s"${p.filePath}").toSet
//
//    // Not doing equals comparison because this directory contains files from other tests.
//    // Also, some stores will prepend a "/" before the filenames. Doing a string comparison to ignore this detail for now.
//    val pathsListed = store.listAll(rootDir).unsafeRunSync().map(_.filePath).toSet.toString()
//    exp.foreach(s => pathsListed must include(s))
//
//    val io: IO[List[Unit]] = paths.map(store.remove).sequence
//    io.unsafeRunSync()
//  }
//
//  it should "list files and directories correctly" in {
//    val dir: Path = dirPath("list-dirs")
//    val paths     = List("subdir/file-1.txt", "file-2.txt").map(writeFile(store, dir))
//    val exp       = paths.map(_.filePath.replaceFirst("/file-1.txt", "")).toSet
//
//    val ls = store.listAll(dir).unsafeRunSync()
//    ls.map(_.filePath).toSet must be(exp)
//    ls.find(_.isDir.getOrElse(false)).map(_.lastSegment) must be(Some("subdir/"))
//
//    val io: IO[List[Unit]] = paths.map(store.remove).sequence
//    io.unsafeRunSync()
//  }
//
//  it should "transfer individual file to a directory from one store to another" in {
//    val srcPath = writeFile(transferStore, dirPath("transfer-single-file-to-dir-src"))("transfer-filename.txt")
//
//    val dstDir  = dirPath("transfer-single-file-to-dir-dst")
//    val dstPath = dstDir / srcPath.lastSegment
//
//    val test = for {
//      i <- transferStore.transferTo(store, srcPath, dstDir)
//      c1 <- transferStore
//        .getContents(srcPath)
//        .handleError(e => s"FAILED transferStore.getContents $srcPath: ${e.getMessage}")
//      c2 <- store.getContents(dstPath).handleError(e => s"FAILED store.getContents $dstPath: ${e.getMessage}")
//      _  <- transferStore.remove(srcPath).handleError(_ => ())
//      _  <- store.remove(dstPath).handleError(_ => ())
//    } yield {
//      i must be(1)
//      c1 must be(c2)
//    }
//
//    test.unsafeRunSync()
//  }
//
//  it should "transfer individual file to a file path from one store to another" in {
//    val srcPath = writeFile(transferStore, dirPath("transfer-file-to-file-src"))("src-filename.txt")
//
//    val dstPath = dirPath("transfer-file-to-file-dst") / "dst-file-name.txt"
//
//    val test = for {
//      i <- transferStore.transferTo(store, srcPath, dstPath)
//      c1 <- transferStore
//        .getContents(srcPath)
//        .handleError(e => s"FAILED transferStore.getContents $srcPath: ${e.getMessage}")
//      c2 <- store
//        .getContents(dstPath)
//        .handleError(e => s"FAILED store.getContents $dstPath: ${e.getMessage}")
//      _ <- transferStore.remove(srcPath).handleError(_ => ())
//      _ <- store.remove(dstPath).handleError(_ => ())
//    } yield {
//      i must be(1)
//      c1 must be(c2)
//    }
//
//    test.unsafeRunSync()
//  }
//
//  it should "transfer directory to a directory path from one store to another" in {
//    val srcDir = dirPath("transfer-dir-to-dir-src")
//    val dstDir = dirPath("transfer-dir-to-dir-dst")
//
//    val paths = (1 to 10).toList
//      .map(i => s"filename-$i.txt")
//      .map(writeFile(transferStore, srcDir))
//
//    val test = for {
//      i <- transferStore.transferTo(store, srcDir, dstDir)
//      c1 <- paths.map { p =>
//        transferStore
//          .getContents(p)
//          .handleError(e => s"FAILED transferStore.getContents $p: ${e.getMessage}")
//      }.sequence
//      c2 <- paths.map { p =>
//        store
//          .getContents(dstDir / p.lastSegment)
//          .handleError(e => s"FAILED store.getContents ${dstDir / p.lastSegment}: ${e.getMessage}")
//      }.sequence
//      _ <- paths.map(transferStore.remove(_).handleError(_ => ())).sequence
//      _ <- paths.map(p => store.remove(dstDir / p.lastSegment).handleError(_ => ())).sequence
//    } yield {
//      i must be(10)
//      c1 must be(c2)
//    }
//
//    test.unsafeRunSync()
//  }
//
//  it should "transfer directories recursively from one store to another" in {
//    val srcDir = dirPath("transfer-dir-rec-src")
//    val dstDir = dirPath("transfer-dir-rec-dst")
//
//    val paths1 = (1 to 5).toList
//      .map(i => s"filename-$i.txt")
//      .map(writeFile(transferStore, srcDir))
//
//    val paths2 = (6 to 10).toList
//      .map(i => s"subdir/filename-$i.txt")
//      .map(writeFile(transferStore, srcDir))
//
//    val paths = paths1 ++ paths2
//
//    val test = for {
//      i <- transferStore.transferTo(store, srcDir, dstDir)
//      c1 <- paths.map { p =>
//        transferStore.getContents(p).handleError(e => s"FAILED transferStore.getContents $p: ${e.getMessage}")
//      }.sequence
//      c2 <- {
//        paths1.map { p =>
//          store
//            .getContents(dstDir / p.lastSegment)
//            .handleError(e => s"FAILED store.getContents ${dstDir / p.lastSegment}: ${e.getMessage}")
//        } ++
//          paths2.map { p =>
//            store
//              .getContents(dstDir / "subdir" / p.lastSegment)
//              .handleError(e => s"FAILED store.getContents ${dstDir / "subdir" / p.lastSegment}: ${e.getMessage}")
//          }
//      }.sequence
//      _ <- paths.map(transferStore.remove(_).handleError(_ => ())).sequence
//      _ <- paths1.map(p => store.remove(dstDir / p.lastSegment).handleError(_ => ())).sequence
//      _ <- paths2.map(p => store.remove(dstDir / "subdir" / p.lastSegment).handleError(_ => ())).sequence
//    } yield {
//      i must be(10)
//      c1.mkString("\n") must be(c2.mkString("\n"))
//    }
//
//    test.unsafeRunSync()
//  }
//
//  it should "copy files in a store from one directory to another" in {
//    val srcDir = dirPath("copy-dir-to-dir-src")
//    val dstDir = dirPath("copy-dir-to-dir-dst")
//
//    writeFile(store, srcDir)("filename.txt")
//
//    val test = for {
//      _ <- store.copy(srcDir / "filename.txt", dstDir / "filename.txt")
//      c1 <- store
//        .getContents(srcDir / "filename.txt")
//        .handleError(e => s"FAILED getContents: ${e.getMessage}")
//      c2 <- store
//        .getContents(dstDir / "filename.txt")
//        .handleError(e => s"FAILED getContents: ${e.getMessage}")
//      _ <- store.remove(dstDir / "filename.txt")
//      _ <- store.remove(srcDir / "filename.txt")
//    } yield {
//      c1.mkString("\n") must be(c2.mkString("\n"))
//    }
//
//    test.unsafeRunSync()
//  }
//
//  // TODO this doesn't test recursive directories. Once listRecursively() is implemented we can fix this
//  it should "remove all should remove all files in a directory" in {
//    val srcDir = dirPath("rm-dir-to-dir-src")
//
//    (1 to 10).toList
//      .map(i => s"filename-$i.txt")
//      .map(writeFile(store, srcDir))
//
//    store.removeAll(srcDir).unsafeRunSync()
//
//    store.list(srcDir).compile.drain.unsafeRunSync().isEmpty must be(true)
//  }
//
//  it should "succeed on remove when path does not exist" in {
//    val dir  = dirPath("remove-nonexistent-path")
//    val path = dir / "no-file.txt"
//    store.remove(path).unsafeRunSync()
//  }
//
//  it should "support putting content with no size" in {
//    val dir: Path = dirPath("put-no-size")
//    val path      = dir / "no-size.txt"
//    val exp       = contents("put without size")
//    val test = for {
//      _ <- fs2
//        .Stream(exp)
//        .covary[IO]
//        .through(fs2.text.utf8Encode)
//        .through(store.put(path))
//        .compile
//        .drain
//      res <- store.getContents(path)
//      _   <- store.remove(path)
//    } yield res must be(exp)
//
//    test.unsafeRunSync()
//  }
//
//  it should "return failed stream when getting non-existing file" in {
//    val test = for {
//      res <- store.get(dirPath("foo") / "doesnt-exists.txt").attempt.compile.lastOrError
//    } yield res mustBe a[Left[_, _]]
//
//    test.unsafeRunSync()
//  }
//
//  it should "overwrite existing file on put with overwrite" in {
//    val dir: Path = dirPath("overwrite-existing")
//    val path      = writeFile(store, dir)("existing.txt")
//
//    fs2
//      .Stream("new content".getBytes().toIndexedSeq: _*)
//      .through(store.put(path))
//      .compile
//      .drain
//      .unsafeRunSync()
//
//    val content = store
//      .get(path, 1024)
//      .compile
//      .to(Array)
//      .map(bytes => new String(bytes))
//      .unsafeRunSync()
//
//    content mustBe "new content"
//  }
//
//  it should "fail on put to Path with existing file without overwrite" in {
//    val dir: Path = dirPath("fail-no-overwrite")
//    val path      = writeFile(store, dir)("existing.txt")
//
//    val result = fs2
//      .Stream("new content".getBytes().toIndexedSeq: _*)
//      .through(store.put(path, overwrite = false))
//      .compile
//      .drain
//      .attempt
//      .unsafeRunSync()
//
//    result mustBe a[Left[_, _]]
//  }
//
//  it should "put to new Path without overwrite" in {
//    val dir: Path = dirPath("no-overwrite")
//    val path      = dir / "new.txt"
//
//    fs2
//      .Stream("new content".getBytes().toIndexedSeq: _*)
//      .through(store.put(path, overwrite = false))
//      .compile
//      .drain
//      .attempt
//      .unsafeRunSync()
//
//    val content = store
//      .get(path, 1024)
//      .compile
//      .to(Array)
//      .map(bytes => new String(bytes))
//      .unsafeRunSync()
//
//    content mustBe "new content"
//  }
//
//  it should "support paths with spaces" in {
//    val dir: Path = dirPath("path spaces")
//    val path      = writeFile(store, dir)("file with spaces")
//    val result = for {
//      list <- store
//        .list(path)
//        .map(_.withSize(None, reset = false).withLastModified(None, reset = false))
//        .compile
//        .toList
//      get    <- store.get(path, 1024).compile.drain.attempt
//      remove <- store.remove(path).attempt
//    } yield {
//      list must contain only path
//      list.headOption.flatMap(_.fileName) must contain("file with spaces")
//      list.headOption.flatMap(_.pathFromRoot.lastOption) must contain("path spaces")
//      get mustBe a[Right[_, _]]
//      remove mustBe a[Right[_, _]]
//    }
//    result.unsafeRunSync()
//  }
//
//  it should "be able to list recursively" in {
//    val dir: Path = dirPath("list-recursively")
//    val files     = List("a", "b", "c", "sub-folder/d", "sub-folder/sub-sub-folder/e", "x", "y", "z").map(dir / _)
//    val result = for {
//      _     <- files.traverse(p => Stream.emit(0: Byte).through(store.put(p)).compile.drain)
//      paths <- store.list(dir, recursive = true).compile.toList
//    } yield {
//      paths must have size 8
//      paths.flatMap(_.fileName) must contain theSameElementsAs List("a", "b", "c", "d", "e", "x", "y", "z")
//      paths.foreach { p =>
//        p.isDir must contain(false)
//        p.size must contain(1L)
//      }
//    }
//    result.unsafeRunSync()
//  }
//
//  it should "put data while rotating files" in {
//    val fileCount      = 5L
//    val fileLength     = 20
//    val lastFileLength = 10
//    val dir: Path      = dirPath("put-rotating")
//    val bytes          = randomBA(fileLength)
//    val lastFileBytes  = randomBA(lastFileLength)
//    val data = Stream.emit(bytes).repeat.take(fileCount).flatMap(bs => Stream.emits(bs.toIndexedSeq)) ++
//      Stream.emits(lastFileBytes.toIndexedSeq)
//
//    // Check overwrite
//    writeFile(store, dir)("3")
//
//    val test = for {
//      counter <- Ref.of[IO, Int](0)
//      _ <- data
//        .through(store.putRotate(counter.getAndUpdate(_ + 1).map(i => dir / s"$i"), fileLength.toLong))
//        .compile
//        .drain
//      files        <- store.listAll(dir)
//      fileContents <- files.traverse(p => store.get(p, fileLength).compile.to(Array).map(p -> _))
//    } yield {
//      files must have size (fileCount + 1)
//      files.flatMap(_.fileName) must contain allElementsOf (0L to fileCount).map(_.toString)
//      files.foreach { p =>
//        p.isDir must contain(false)
//        p.size must contain(if (p.fileName.contains(fileCount.toString)) lastFileLength else fileLength)
//      }
//      fileContents.foreach {
//        case (p, content) =>
//          content mustBe (if (p.fileName.contains(fileCount.toString)) lastFileBytes else bytes)
//      }
//
//    }
//    test.unsafeRunSync()
//  }

  def dirPath(name: String): Url[Bucket] = Url(scheme, root, testRunRoot `//` name)

  def contents(filename: String): String = s"file contents to upload: $filename"

  def writeFile(store: BlobStore[IO, B], tmpDir: Path.Plain)(filename: String): Url.Bucket = {
    val url = Url(scheme, root, tmpDir / filename)
    store.put(contents(filename), url).compile.drain.unsafeRunSync()
    url
  }

  def randomBA(length: Int): Array[Byte] = {
    val ba = new Array[Byte](length)
    Random.nextBytes(ba)
    ba
  }

  // remove dirs created by AbstractStoreTest
//  override def afterAll(): Unit = cleanup(transferStoreRootDir.resolve(testRunRoot))

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
