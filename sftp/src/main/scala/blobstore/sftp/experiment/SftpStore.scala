package blobstore.sftp.experiment

import com.jcraft.jsch._
import cats.syntax.all._
import cats.instances.option._
import cats.instances.string._
import java.io.OutputStream

import blobstore.experiment.SingleAuthorityStore
import blobstore.experiment.url.Path
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Effect, IO, Resource}
import cats.effect.concurrent.{MVar, Semaphore}
import fs2.{Pipe, Stream}
import fs2.concurrent.Queue

import scala.util.Try

final class SftpStore[F[_]](
  private[sftp] val session: Session,
  blocker: Blocker,
  mVar: MVar[F, ChannelSftp],
  semaphore: Option[Semaphore[F]],
  connectTimeout: Int
)(implicit F: ConcurrentEffect[F], CS: ContextShift[F])
  extends SingleAuthorityStore[F, SftpATTRS] {

  @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
  private val openChannel: F[ChannelSftp] = {
    val openF = blocker.delay {
      val ch = session.openChannel("sftp").asInstanceOf[ChannelSftp]
      ch.connect(connectTimeout)
      ch
    }
    semaphore.fold(openF) { s => s.tryAcquire.ifM(openF, getChannel) }
  }

  private val getChannel = mVar.tryTake.flatMap {
    case Some(channel) => channel.pure[F]
    case None          => openChannel
  }

  private def channelResource: Resource[F, ChannelSftp] = Resource.make(getChannel) {
    case ch if ch.isClosed => ().pure[F]
    case ch                => mVar.tryPut(ch).ifM(().pure[F], SftpStore.closeChannel(semaphore, blocker)(ch))
  }

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, SftpPath] = {

    def entrySelector(cb: ChannelSftp#LsEntry => Unit): ChannelSftp.LsEntrySelector = (entry: ChannelSftp#LsEntry) => {
      cb(entry)
      ChannelSftp.LsEntrySelector.CONTINUE
    }

    val stream = for {
      q       <- Stream.eval(Queue.bounded[F, Option[ChannelSftp#LsEntry]](64))
      channel <- Stream.resource(channelResource)
      entry <- q.dequeue.unNoneTerminate
        .filter(e => e.getFilename != "." && e.getFilename != "..")
        .concurrently(
          Stream.eval {
            val es      = entrySelector(e => Effect[F].runAsync(q.enqueue1(Some(e)))(_ => IO.unit).unsafeRunSync())
            blocker.delay(channel.ls(path.show, es)).attempt.flatMap(_ => q.enqueue1(None))
          }
        )
    } yield {
      val isDir = Option(entry.getAttrs.isDir)
      val newPath =
        if (path.fileName.contains(entry.getFilename)) path
        else path / (entry.getFilename ++ (if (isDir.contains(true)) "/" else ""))

      SftpPath(newPath.root, newPath.fileName, newPath.pathFromRoot, entry.getAttrs)
    }
    if (recursive) {
      stream.flatMap {
        case p if p.isDir.contains(true) => list(p, recursive)
        case p                           => Stream.emit(p)
      }
    } else {
      stream
    }
  }

  override def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte] =
    Stream.resource(channelResource).flatMap { channel =>
      fs2.io.readInputStream(
        blocker.delay(channel.get(path.show)),
        chunkSize = chunkSize,
        closeAfterUse = true,
        blocker = blocker
      )
    }

  override def put[A](path: Path[A], overwrite: Boolean = true): Pipe[F, Byte, Unit] = { in =>
    def pull(channel: ChannelSftp): Stream[F, Unit] =
      Stream
        .resource(outputStreamResource(channel, path, overwrite))
        .flatMap(os => _writeAllToOutputStream1(in, os, blocker).stream)

    Stream.resource(channelResource).flatMap(channel => pull(channel))
  }

  override def move[A](src: Path[A], dst: Path[A]): F[Unit] = channelResource.use { channel =>
    mkdirs(dst, channel) >> blocker.delay(
      channel.rename(src.show, dst.show)
    )
  }

  override def copy[A](src: Path[A], dst: Path[A]): F[Unit] = {
    val s = for {
      channel <- Stream.resource(channelResource)
      _       <- Stream.eval(mkdirs(dst, channel))
      _       <- get(src).through(this.bufferedPut(dst, blocker))
    } yield ()

    s.compile.drain
  }

  override def remove[A](path: Path[A]): F[Unit] = channelResource.use { channel =>
    blocker.delay {
      try {
        if (path.isDir.getOrElse(path.fileName.isEmpty)) channel.rmdir(path.show) else channel.rm(path.show)
      } catch {
        // Let the remove() call succeed if there is no file at this path
        case e: SftpException if e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE => ()
      }
    }
  }

  override def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, OutputStream] =
      for {
        p       <- Resource.liftF(computePath)
        channel <- channelResource
        os      <- outputStreamResource(channel, p)
      } yield os

    putRotateBase(limit, openNewFile)(os => bytes => blocker.delay(os.write(bytes.toArray)))
  }

  private def mkdirs[A](path: Path[A], channel: ChannelSftp): F[Unit] =
    blocker.delay {
      val root       = if (path.show.startsWith("/")) "/" else ""

      path.segments.toList.foldLeft(root) {
        case (acc, s) =>
          Try(channel.mkdir(acc))
          val candidate = List(acc, s).filter(_.nonEmpty).mkString("/")
          // Don't .replaceAll("//"). Directories containing "//" are valid according to spec
          if (root == "/" && candidate.startsWith("//")) candidate.replaceFirst("//", "/")
          else candidate
      }
      ()
    }

  private def outputStreamResource(
    channel: ChannelSftp,
    path: Path,
    overwrite: Boolean = true
  ): Resource[F, OutputStream] = {
    def put(channel: ChannelSftp): F[OutputStream] = {
      val newOrOverwrite =
        mkdirs(path, channel) >> blocker.delay(channel.put(SftpStore.strPath(absRoot, path), ChannelSftp.OVERWRITE))
      if (overwrite) {
        newOrOverwrite
      } else {
        blocker.delay(channel.ls(SftpStore.strPath(absRoot, path))).attempt.flatMap {
          case Left(e: SftpException) if e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE =>
            newOrOverwrite
          case Left(e)  => F.raiseError(e)
          case Right(_) => F.raiseError(new IllegalArgumentException(s"File at path '$path' already exist."))
        }
      }
    }

    def close(os: OutputStream): F[Unit] = blocker.delay(os.close())

    Resource.make(put(channel))(close)
  }
}

object SftpStore {

  /**
    * Safely initialize SftpStore and disconnect ChannelSftp and Session upon finish.
    *
    * @param fa F[ChannelSftp] how to connect to SFTP server
    * @return Stream[ F, SftpStore[F] ] stream with one SftpStore, sftp channel will disconnect once stream is done.
    */
  def apply[F[_]: ConcurrentEffect](
    fa: F[Session],
    blocker: Blocker,
    maxChannels: Option[Long] = None,
    connectTimeout: Int = 10000
  )(implicit CS: ContextShift[F]): Stream[F, SftpStore[F]] =
    if (maxChannels.exists(_ < 1)) {
      Stream.raiseError[F](new IllegalArgumentException(s"maxChannels must be >= 1"))
    } else
      for {
        session <- {
          val attemptConnect = fa.flatTap { session =>
            blocker.delay(session.connect()).recover {
              case e: JSchException if e.getMessage == "session is already connected" => ()
            }
          }
          Stream.bracket(attemptConnect)(session => blocker.delay(session.disconnect()))
        }
        semaphore <- Stream.eval(maxChannels.traverse(Semaphore.apply[F]))
        mVar <- Stream.bracket(MVar.empty[F, ChannelSftp])(mVar =>
          mVar.tryTake.flatMap(_.fold(().pure[F])(closeChannel[F](semaphore, blocker)))
        )
      } yield new SftpStore[F](absRoot, session, blocker, mVar, semaphore, connectTimeout)

  private def closeChannel[F[_]](semaphore: Option[Semaphore[F]], blocker: Blocker)(
    ch: ChannelSftp
  )(implicit F: ConcurrentEffect[F], CS: ContextShift[F]): F[Unit] =
    F.productR(semaphore.fold(F.unit)(_.release))(blocker.delay(ch.disconnect()))
}
