package blobstore.sftp

import java.io.OutputStream

import blobstore.{_writeAllToOutputStream1, putRotateBase, FileStore}
import blobstore.url.{Authority, Path}
import blobstore.url.Path.{AbsolutePath, RootlessPath}
import blobstore.url.exception.MultipleUrlValidationException
import blobstore.Store.{DelegatingStore, UniversalStore}
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Effect, IO, Resource}
import cats.effect.concurrent.{MVar, Semaphore}
import cats.instances.option._
import cats.syntax.all._
import com.jcraft.jsch._
import fs2.{Pipe, Stream}
import fs2.concurrent.Queue

import scala.util.Try

final class SftpStore[F[_]](
  val authority: Authority,
  private[sftp] val session: Session,
  blocker: Blocker,
  mVar: MVar[F, ChannelSftp],
  semaphore: Option[Semaphore[F]],
  connectTimeout: Int
)(implicit F: ConcurrentEffect[F], CS: ContextShift[F])
  extends FileStore[F, SftpFsElement] {

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

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[SftpFsElement]] = {

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
            val es = entrySelector(e => Effect[F].runAsync(q.enqueue1(Some(e)))(_ => IO.unit).unsafeRunSync())
            blocker.delay(channel.ls(path.show, es)).attempt.flatMap(_ => q.enqueue1(None))
          }
        )
    } yield {
      val isDir   = Option(entry.getAttrs.isDir)
      val element = SftpFsElement(entry.getLongname, entry.getAttrs)

      if (path.fileName.contains(entry.getFilename)) path.as(element)
      else path.addSegment(entry.getFilename ++ (if (isDir.contains(true)) "/" else ""), element)
    }
    if (recursive) {
      stream.flatMap {
        case p if p.isDir => list(p, recursive)
        case p            => Stream.emit(p)
      }
    } else stream
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

      // TODO: Implement this without flushing to disk
      //      _       <- get(src, 4096).through(this.bufferedPut(dst, blocker))
    } yield ()

    s.compile.drain
  }

  override def remove[A](path: Path[A]): F[Unit] = channelResource.use { channel =>
    blocker.delay(channel.stat(path.show)).flatMap { stat =>
    {
      val rm = if (stat.isDir) blocker.delay(channel.rmdir(path.show)) else blocker.delay(channel.rm(path.show))
      rm.handleErrorWith {
        case e: SftpException if e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE => ().pure[F]
        case e                                                           => e.raiseError[F, Unit]
      }
    }
    }
  }

  override def putRotate(computePath: F[Path.Plain], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, OutputStream] =
      for {
        p       <- Resource.liftF(computePath)
        channel <- channelResource
        os      <- outputStreamResource(channel, p)
      } yield os

    putRotateBase(limit, openNewFile)(os => bytes => blocker.delay(os.write(bytes.toArray)))
  }

  private def mkdirs[A](path: Path[A], channel: ChannelSftp): F[Unit] = {
    val root: Path.Plain = path match {
      case AbsolutePath(_, _) => AbsolutePath.root
      case RootlessPath(_, _) => RootlessPath.relativeHome
    }

    fs2.Stream.emits(path.segments.toList)
      .covary[F]
      .evalScan(root) { (acc, el) =>
        blocker.delay(Try(channel.mkdir(acc.show))).as(acc / el)
      }
      .compile
      .drain
  }

  private def outputStreamResource[A](
    channel: ChannelSftp,
    path: Path[A],
    overwrite: Boolean = true
  ): Resource[F, OutputStream] = {
    def put(channel: ChannelSftp): F[OutputStream] = {
      val newOrOverwrite =
        mkdirs(path, channel) >> blocker.delay(channel.put(path.show, ChannelSftp.OVERWRITE))
      if (overwrite) {
        newOrOverwrite
      } else {
        blocker.delay(channel.ls(path.show)).attempt.flatMap {
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

  override val liftToUniversal: UniversalStore[F] = new DelegatingStore[F, SftpFsElement](SftpFsElement.toGeneral, Right(this))
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
        authority <- Stream.eval(Authority.parse(session.getHost).leftMap(MultipleUrlValidationException.apply).liftTo[F])
        semaphore <- Stream.eval(maxChannels.traverse(Semaphore.apply[F]))
        mVar <- Stream.bracket(MVar.empty[F, ChannelSftp])(mVar =>
          mVar.tryTake.flatMap(_.fold(().pure[F])(closeChannel[F](semaphore, blocker)))
        )
      } yield new SftpStore[F](authority, session, blocker, mVar, semaphore, connectTimeout)

  private def closeChannel[F[_]](semaphore: Option[Semaphore[F]], blocker: Blocker)(
    ch: ChannelSftp
  )(implicit F: ConcurrentEffect[F], CS: ContextShift[F]): F[Unit] =
    F.productR(semaphore.fold(F.unit)(_.release))(blocker.delay(ch.disconnect()))
}
