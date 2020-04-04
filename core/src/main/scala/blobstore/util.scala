package blobstore

import java.util.concurrent.{CancellationException, CompletableFuture, CompletionException}
import cats.effect.Concurrent
import cats.syntax.flatMap._
import cats.syntax.functor._

object util {
  def liftJavaFuture[F[_], A](fa: F[CompletableFuture[A]])(implicit F: Concurrent[F]): F[A] = fa.flatMap { cf =>
    F.cancelable { cb =>
      cf.handle[Unit]((result: A, err: Throwable) =>
        Option(err) match {
          case None =>
            cb(Right(result))
          case Some(_: CancellationException) =>
            ()
          case Some(ex: CompletionException) =>
            cb(Left(Option(ex.getCause).getOrElse(ex)))
          case Some(ex) =>
            cb(Left(ex))
        }
      )
      F.delay(cf.cancel(true)).void
    }
  }
}
