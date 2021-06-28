package blobstore

import cats.effect.concurrent.Ref
import cats.effect.{IO, Resource}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.Assertion

import scala.util.Random

class RotateTest extends AnyFlatSpec {
  import RotateTest._

  behavior of "putRotateBase"

  it should "not allocate resource on empty stream" in {
    testRotate(0, 0, 0).unsafeRunSync()
  }

  it should "not allocate resource for empty last part" in {
    testRotate(6, 3, 3).unsafeRunSync()
  }
}

object RotateTest extends Matchers with IOTest {

  def testRotate(chunks: Long, allocs: Int, deallocs: Int): IO[Assertion] = for {
    allocationCounter   <- Ref.of[IO, Int](0)
    deallocationCounter <- Ref.of[IO, Int](0)
    resource = Resource.make(allocationCounter.update(_ + 1))(_ => deallocationCounter.update(_ + 1))
    r        = Random
    _ <- fs2.Stream.repeatEval(IO(r.nextBytes(50))).take(chunks).flatMap(bs => fs2.Stream.emits(bs)).through(
      putRotateBase(100, resource)(_ => _ => IO.unit)
    ).compile.drain
    allocations   <- allocationCounter.get
    deallocations <- deallocationCounter.get
  } yield {
    allocations mustBe allocs
    deallocations mustBe deallocs
  }
}
