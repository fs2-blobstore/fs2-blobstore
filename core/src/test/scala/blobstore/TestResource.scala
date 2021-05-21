package blobstore

import blobstore.fs.NioPath
import blobstore.url.{FsObject, Url}
import cats.effect.IO

import scala.concurrent.duration.FiniteDuration

final case class TestResource[B <: FsObject, T](
  store: Store[IO, B],
  extra: T,
  timeout: FiniteDuration,
  transferStoreRoot: Url[NioPath],
  transferStore: Store[IO, NioPath],
  disableHighRateTests: Boolean = false
)
