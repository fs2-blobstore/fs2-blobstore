package blobstore

import cats.Functor
import cats.effect.Concurrent

import java.util.concurrent.{CancellationException, CompletableFuture, CompletionException}
import cats.syntax.all.*
import fs2.Chunk
import fs2.concurrent.Queue

object util {
  def liftJavaFuture[F[_]: Concurrent, A](fa: F[CompletableFuture[A]]): F[A] = fa.flatMap { cf =>
    Concurrent[F].cancelable { cb =>
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
      Concurrent[F].delay(cf.cancel(true)).void
    }
  }

  private def fromQueueNoneTerminatedChunk_[F[_], A](
    take: F[Option[Chunk[A]]],
    tryTake: F[Option[Option[Chunk[A]]]],
    limit: Int
  ): fs2.Stream[F, A] = {
    def await: fs2.Stream[F, A] =
      fs2.Stream.eval(take).flatMap {
        case None    => fs2.Stream.empty
        case Some(c) => pump(c)
      }
    def pump(acc: Chunk[A]): fs2.Stream[F, A] = {
      val sz = acc.size
      if (sz > limit) {
        val (pfx, sfx) = acc.splitAt(limit)
        fs2.Stream.chunk(pfx) ++ pump(sfx)
      } else if (sz == limit) fs2.Stream.chunk(acc) ++ await
      else
        fs2.Stream.eval(tryTake).flatMap {
          case None          => fs2.Stream.chunk(acc) ++ await
          case Some(Some(c)) => pump(Chunk.concat(List(acc, c)))
          case Some(None)    => fs2.Stream.chunk(acc)
        }
    }
    await
  }

  def fromQueueNoneTerminatedChunk[F[_], A](
    queue: Queue[F, Option[Chunk[A]]],
    limit: Int = Int.MaxValue
  ): fs2.Stream[F, A] =
    fromQueueNoneTerminatedChunk_(queue.dequeue1, queue.tryDequeue1, limit)

  def fromQueueNoneTerminated[F[_]: Functor, A](
    queue: Queue[F, Option[A]],
    limit: Int = Int.MaxValue
  ): fs2.Stream[F, A] =
    fromQueueNoneTerminatedChunk_(
      queue.dequeue1.map(_.map(Chunk.singleton)),
      queue.tryDequeue1.map(_.map(_.map(Chunk.singleton))),
      limit
    )
}
