package blobstore.s3

import blobstore.AbstractStoreTest
import blobstore.url.{Path, Url}
import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import org.scalatest.{Assertion, Inside}

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

abstract class AbstractS3StoreTest extends AbstractStoreTest[S3Blob] with Inside {

  override val scheme: String             = "s3"
  override val fileSystemRoot: Path.Plain = Path("")

  def s3Store: S3Store[IO] = store.asInstanceOf[S3Store[IO]] // scalafix:ok

  behavior of "S3Store"

  it should "expose underlying metadata" in {
    val dir  = dirUrl("expose-underlying")
    val path = writeFile(store, dir)("abc.txt")

    val entities = store.list(path).compile.toList.unsafeRunSync()

    entities.foreach { s3Url =>
      inside(s3Url.path.representation.meta) {
        case Some(metaInfo) =>
          // Note: defaultFullMetadata = true in S3Store constructor.
          metaInfo.contentType mustBe a[Some[?]]
          metaInfo.eTag mustBe a[Some[?]]
      }
    }
  }

  def testUploadNoSize(size: Long, name: String): IO[Assertion] = {
    val arr = new Array[Byte](size.toInt)
    Random.nextBytes(arr)
    for {
      bytes <- Stream.emits(arr).covary[IO].take(size).compile.to(Array)
      path = testRunRoot / "multipart-upload" / name
      url  = Url("s3", authority, path)
      result    <- Stream.chunk(Chunk.bytes(bytes)).through(store.put(url, size = None)).compile.drain.attempt
      _         <- IO.sleep(FiniteDuration(5, "s"))
      readBytes <- store.get(url, 4096).compile.to(Array)
      _         <- store.remove(url)
    } yield {
      result.isRight mustBe true
      readBytes mustBe bytes
    }
  }

  it should "put content with no size when aligned with multi-upload boundaries 5mb" in {
    testUploadNoSize(5 * 1024 * 1024, "5mb").unsafeRunSync()
  }

  it should "put content with no size when aligned with multi-upload boundaries 10mb" in {
    testUploadNoSize(10 * 1024 * 1024, "10mb").unsafeRunSync()
  }

  it should "put content with no size when not aligned with multi-upload boundaries 7mb" in {
    testUploadNoSize(7 * 1024 * 1024, "7mb").unsafeRunSync()
  }

  it should "put content with no size when not aligned with multi-upload boundaries 12mb" in {
    testUploadNoSize(12 * 1024 * 1024, "12mb").unsafeRunSync()
  }

  it should "put rotating with file-limit > bufferSize" in {
    val dir     = dirUrl("put-rotating-s3")
    val content = randomBA(7 * 1024 * 1024)
    val data    = Stream.emits(content)

    val test = for {
      counter <- Ref.of[IO, Int](0)
      _ <- data
        .through(store.putRotate(counter.getAndUpdate(_ + 1).map(i => dir / i.toString), 6 * 1024 * 1024))
        .compile
        .drain
      files        <- store.list(dir, recursive = true).compile.toList
      fileContents <- files.traverse(u => store.get(u, 1024).compile.toList)
    } yield {
      files must have size 2
      files.flatMap(_.path.size) must contain theSameElementsAs List(6 * 1024 * 1024L, 1024 * 1024L)
      fileContents.flatten must contain theSameElementsInOrderAs content.toList
    }

    test.unsafeRunSync()
  }

}
