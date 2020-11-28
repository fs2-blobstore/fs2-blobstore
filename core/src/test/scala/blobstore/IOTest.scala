package blobstore

import cats.effect.{Blocker, ContextShift, IO, Timer}
import cats.effect.internals.ExtractCatsEC

trait IOTest {
  protected implicit def contextShift: ContextShift[IO] = IOTest.testContextShift

  protected implicit def timer: Timer[IO] = IOTest.testTimer

  protected def blocker: Blocker = IOTest.blocker
}

object IOTest {

  /** Let the lifecycle of this be equal to the life cycle of the JVM that run tests
    */
  private lazy val blocker: Blocker = Blocker[IO].allocated.map(_._1).unsafeRunSync()

  private lazy val testContextShift = IO.contextShift(ExtractCatsEC.executionContext)

  private lazy val testTimer = IO.timer(ExtractCatsEC.executionContext, ExtractCatsEC.scheduledExecutorService)
}
