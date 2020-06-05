package blobstore.gcs

import java.io.OutputStream

import cats.effect.{ConcurrentEffect, Effect}
import fs2.{Chunk, Stream}
import fs2.concurrent.Queue
import cats.syntax.functor._

import scala.annotation.tailrec

private[gcs] class Fs2OutputStream[F[_]](
  queue: Queue[F, Option[Chunk[Byte]]],
  chunkSize: Int
)(implicit eff: Effect[F])
  extends OutputStream {
  @SuppressWarnings(Array("scalafix:DisableSyntax.var"))
  private var bufferedChunk: Chunk[Byte] = Chunk.empty

  @tailrec
  private def addChunk(newChunk: Chunk[Byte]): Unit = {
    val newChunkSize         = newChunk.size
    val bufferedChunkSize    = bufferedChunk.size
    val spaceLeftInTheBuffer = chunkSize - bufferedChunkSize

    if (newChunkSize > spaceLeftInTheBuffer) {
      bufferedChunk = Chunk.concatBytes(Seq(bufferedChunk, newChunk.take(spaceLeftInTheBuffer)), chunkSize)
      flushBuffer()
      addChunk(newChunk.drop(spaceLeftInTheBuffer))
    } else {
      bufferedChunk =
        Chunk.concatBytes(Seq(bufferedChunk, newChunk), bufferedChunkSize + newChunkSize)
    }
  }

  private def flushBuffer(): Unit = {
    enqueueChunkSync(Some(bufferedChunk))
    bufferedChunk = Chunk.empty
  }

  private def enqueueChunkSync(c: Option[Chunk[Byte]]): Unit =
    eff.toIO(queue.enqueue1(c)).unsafeRunSync()

  val stream: Stream[F, Byte] = queue.dequeue.unNoneTerminate.flatMap(Stream.chunk)

  override def write(bytes: Array[Byte]): Unit =
    addChunk(Chunk.bytes(bytes))
  override def write(bytes: Array[Byte], off: Int, len: Int): Unit =
    addChunk(Chunk.bytes(bytes, off, len))
  override def write(b: Int): Unit =
    addChunk(Chunk.singleton(b.toByte))

  override def flush(): Unit = flushBuffer()

  override def close(): Unit = {
    flush()
    enqueueChunkSync(None)
  }
}

private[gcs] object Fs2OutputStream {
  def apply[F[_]: ConcurrentEffect](chunkSize: Int, queueSize: Option[Int]): F[Fs2OutputStream[F]] = {
    val fQueue = queueSize match {
      case None       => Queue.unbounded[F, Option[Chunk[Byte]]]
      case Some(size) => Queue.bounded[F, Option[Chunk[Byte]]](size)
    }
    fQueue.map { queue => new Fs2OutputStream[F](queue, chunkSize) }
  }
}
