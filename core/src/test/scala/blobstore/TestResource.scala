package blobstore

import blobstore.url.FsObject
import cats.effect.IO

import scala.concurrent.duration.FiniteDuration

final case class TestResource[B <: FsObject, T](
  store: Store[IO, B],
  extra: T,
  timeout: FiniteDuration,
  disableHighRateTests: Boolean = false
)
