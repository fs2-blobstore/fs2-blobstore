package blobstore

import blobstore.url.FsObject
import cats.effect.IO

final case class TestResource[B <: FsObject, T](
  store: Store[IO, B],
  extra: T,
  disableHighRateTests: Boolean = false
)
