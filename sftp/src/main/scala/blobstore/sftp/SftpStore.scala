package blobstore.sftp

import blobstore.{_writeAllToOutputStream1, defaultTransferTo, putRotateBase, PathStore, Store}
import blobstore.url.{Authority, FsObject, Path, Url}
import blobstore.url.Path.{AbsolutePath, Plain, RootlessPath}
import blobstore.url.exception.MultipleUrlValidationException
import cats.data.Validated
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Effect, IO, Resource}
import cats.effect.concurrent.{MVar, MVar2, Semaphore}
import cats.syntax.all._
import com.jcraft.jsch._
import fs2.{Pipe, Stream}
import fs2.concurrent.Queue

import java.io.OutputStream
import scala.util.Try

class SftpStore[F[_]] private (
  val authority: Authority,
  private[sftp] val session: Session,
  blocker: Blocker,
  mVar: MVar2[F, ChannelSftp],
  semaphore: Option[Semaphore[F]],
  connectTimeout: Int
)(implicit F: ConcurrentEffect[F], CS: ContextShift[F])
  extends PathStore[F, SftpFile] {

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

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[SftpFile]] = {

    def entrySelector(cb: ChannelSftp#LsEntry => Unit): ChannelSftp.LsEntrySelector = (entry: ChannelSftp#LsEntry) => {
      cb(entry)
      ChannelSftp.LsEntrySelector.CONTINUE
    }

    val stream = for {
      q       <- Stream.eval(Queue.bounded[F, Option[ChannelSftp#LsEntry]](64))
      channel <- Stream.resource(channelResource)
      entry <- q.dequeue.unNoneTerminate
        .filter(e => e.getFilename != "." && e.getFilename != "..")
        .concurrently {
          // JSch hits index out of bounds on empty string, needs explicit handling
          val resolveEmptyPath = if (path.show.isEmpty) blocker.delay(channel.pwd()) else path.show.pure[F]
          val performList = resolveEmptyPath.flatMap { path =>
            val es = entrySelector(e => Effect[F].runAsync(q.enqueue1(Some(e)))(_ => IO.unit).unsafeRunSync())
            blocker.delay(channel.ls(path.show, es)).attempt.flatMap(_ => q.enqueue1(None))
          }
          Stream.eval(performList)
        }
    } yield {
      val isDir   = Option(entry.getAttrs.isDir)
      val element = SftpFile(entry.getLongname, entry.getAttrs)

      if (path.lastSegment.contains(entry.getFilename)) path.as(element)
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

  override def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = {
    in =>
      def pull(channel: ChannelSftp): Stream[F, Unit] =
        Stream
          .resource(outputStreamResource(channel, path, overwrite))
          .flatMap(os => _writeAllToOutputStream1(in, os, blocker).stream)

      Stream.resource(channelResource).flatMap(channel => pull(channel))
  }

  override def move[A, B](src: Path[A], dst: Path[B]): F[Unit] = channelResource.use { channel =>
    mkdirs(dst, channel) >> blocker.delay(
      channel.rename(src.show, dst.show)
    )
  }

  override def copy[A, B](src: Path[A], dst: Path[B]): F[Unit] = channelResource.use { channel =>
    mkdirs(dst, channel) >> get(src, 4096).through(put(dst)).compile.drain
  }

  override def remove[A](path: Path[A], recursive: Boolean = false): F[Unit] = channelResource.use { channel =>
    def recursiveRemove(path: Path[SftpFile]): F[Unit] = {
      //TODO: Parallelize this with multiple channels
      val r =
        if (path.isDir) {
          list(path).evalMap(recursiveRemove) ++ Stream.eval(blocker.delay(channel.rmdir(path.show)))
        } else Stream.eval(blocker.delay(channel.rm(path.show)))
      r.compile.drain
    }

    _stat(path, channel).flatMap {
      case Some(p) =>
        if (recursive) recursiveRemove(p)
        else {
          if (p.isDir) blocker.delay(channel.rmdir(p.show)) else blocker.delay(channel.rm(p.show))
        }
      case None => ().pure[F]
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

  private def mkdirs[A](path: Path[A], channel: ChannelSftp): F[Unit] = {
    val root: Path.Plain = path match {
      case AbsolutePath(_, _) => AbsolutePath.root
      case RootlessPath(_, _) => RootlessPath.root
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

  override def stat[A](path: Path[A]): F[Option[Path[SftpFile]]] =
    channelResource.use { channel =>
      _stat(path, channel)
    }

  def _stat[A](path: Path[A], channel: ChannelSftp): F[Option[Path[SftpFile]]] =
    blocker.delay(channel.stat(path.show))
      .map(a => path.as(SftpFile(path.show, a)).some).handleErrorWith {
        case e: SftpException if e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE => none[Path[SftpFile]].pure[F]
        case e                                                           => e.raiseError[F, Option[Path[SftpFile]]]
      }

  override def lift: Store[F, SftpFile] =
    lift((u: Url) => u.path.relative.valid)

  override def lift(g: Url => Validated[Throwable, Plain]): Store[F, SftpFile] =
    new Store.DelegatingStore[F, SftpFile](this, g)

  override def transferTo[B, P](dstStore: Store[F, B], srcPath: Path[P], dstUrl: Url)(implicit
  ev: B <:< FsObject): F[Int] =
    defaultTransferTo(this, dstStore, srcPath, dstUrl)

  override def getContents[A](path: Path[A], chunkSize: Int): F[String] =
    get(path, chunkSize).through(fs2.text.utf8Decode).compile.string
}

object SftpStore {

  /** Safely initialize SftpStore and disconnect ChannelSftp and Session upon finish.
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
        authority <- {
          val port = Option(session.getPort).filter(_ != 22).map(":" + _).getOrElse("")
          Stream.eval(
            Authority.parse(session.getHost + port).leftMap(MultipleUrlValidationException.apply).liftTo[F]
          )
        }
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
