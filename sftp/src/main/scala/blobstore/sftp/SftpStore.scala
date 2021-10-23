package blobstore.sftp

import blobstore.{defaultTransferTo, putRotateBase, PathStore, Store}
import blobstore.url.{Authority, FsObject, Path, Url}
import blobstore.url.Path.{AbsolutePath, Plain, RootlessPath}
import blobstore.url.exception.MultipleUrlValidationException
import blobstore.util.fromQueueNoneTerminated
import cats.data.Validated
import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource}
import cats.syntax.all._
import com.jcraft.jsch._
import fs2.concurrent.Queue
import fs2.{Pipe, Stream}

import java.io.OutputStream
import scala.util.Try

/** Safely initialize SftpStore and disconnect Session upon finish.
  *
  * @param authority
  *   – process used to connect to SFTP server.
  * @param session
  *   – connected jsch Session.
  * @param queue
  *   – queue to hold channels to be reused
  * @param semaphore
  *   – optional semaphore to limit the number of concurrently open channels.
  * @param connectTimeout
  *   – override for channel connect timeout.
  */
class SftpStore[F[_]: ConcurrentEffect: ContextShift] private (
  val authority: Authority,
  private[sftp] val session: Session,
  blocker: Blocker,
  queue: Queue[F, ChannelSftp],
  semaphore: Option[Semaphore[F]],
  connectTimeout: Int
) extends PathStore[F, SftpFile] {

  private val getChannel: F[ChannelSftp] =
    queue.tryDequeue1.flatMap {
      case Some(channel) => channel.pure[F]
      case None =>
        val openF = blocker.delay {
          val ch = session.openChannel("sftp").asInstanceOf[ChannelSftp] // scalafix:ok
          ch.connect(connectTimeout)
          ch
        }
        semaphore.fold(openF) { s => s.tryAcquire.ifM(openF, getChannel) }
    }

  def closeChannel(ch: ChannelSftp): F[Unit] = semaphore match {
    case Some(s) => s.release.flatMap(_ => blocker.delay(ch.disconnect()))
    case None    => ().pure
  }

  private def channelResource: Resource[F, ChannelSftp] = Resource.make(getChannel) {
    case ch if ch.isClosed => ().pure[F]
    case ch                => queue.offer1(ch).ifM(().pure[F], closeChannel(ch))
  }

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[SftpFile]] = {

    def entrySelector(cb: ChannelSftp.LsEntry => Unit): ChannelSftp.LsEntrySelector = (entry: ChannelSftp.LsEntry) => {
      cb(entry)
      ChannelSftp.LsEntrySelector.CONTINUE
    }

    val stream = for {
      q       <- Stream.eval(Queue.bounded[F, Option[ChannelSftp.LsEntry]](64))
      channel <- Stream.resource(channelResource)
      entry <- fromQueueNoneTerminated(q)
        .filter(e => e.getFilename != "." && e.getFilename != "..")
        .concurrently {
          // JSch hits index out of bounds on empty string, needs explicit handling
          val resolveEmptyPath = if (path.show.isEmpty) blocker.delay(channel.pwd()) else path.show.pure[F]
          val performList = resolveEmptyPath.flatMap { path =>
            val es = entrySelector(e =>
              ConcurrentEffect[F].runAsync(q.enqueue1(Some(e)))(_ => cats.effect.IO.unit).unsafeRunSync()
            )
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
        blocker = blocker,
        closeAfterUse = true
      )
    }

  override def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = {
    in =>
      def pull(channel: ChannelSftp): Stream[F, Unit] =
        Stream
          .resource(outputStreamResource(channel, path, overwrite))
          .flatMap(os => in.through(fs2.io.writeOutputStream(os.pure, blocker, closeAfterUse = false)))

      Stream.resource(channelResource).flatMap(channel => pull(channel))
  }

  override def move[A, B](src: Path[A], dst: Path[B]): F[Unit] = channelResource.use { channel =>
    mkdirs(dst, channel) >> blocker.delay(channel.rename(src.show, dst.show))
  }

  override def copy[A, B](src: Path[A], dst: Path[B]): F[Unit] = channelResource.use { channel =>
    mkdirs(dst, channel) >> get(src, 4096).through(put(dst)).compile.drain
  }

  override def remove[A](path: Path[A], recursive: Boolean = false): F[Unit] = {
    def recursiveRemove(path: Path[SftpFile]): F[Unit] = channelResource.use { channel =>
      val r =
        if (path.isDir) {
          list(path).evalMap(recursiveRemove) ++ Stream.eval(blocker.delay(channel.rmdir(path.show)))
        } else Stream.eval(blocker.delay(channel.rm(path.show)))
      r.compile.drain
    }
    channelResource.use { channel =>
      _stat(path, channel).flatMap {
        case Some(p) =>
          if (recursive) recursiveRemove(p)
          else {
            if (p.isDir) blocker.delay(channel.rmdir(p.show)) else blocker.delay(channel.rm(p.show))
          }
        case None => ().pure[F]
      }
    }
  }

  override def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] = {
    val openNewFile: Resource[F, OutputStream] =
      for {
        p       <- Resource.eval(computePath)
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
          case Left(e) => ConcurrentEffect[F].raiseError(e)
          case Right(_) =>
            ConcurrentEffect[F].raiseError(new IllegalArgumentException(s"File at path '$path' already exist."))
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
    lift((u: Url.Plain) => u.path.relative.valid)

  override def lift(g: Url.Plain => Validated[Throwable, Plain]): Store[F, SftpFile] =
    new Store.DelegatingStore[F, SftpFile](this, g)

  override def transferTo[B, P, A](dstStore: Store[F, B], srcPath: Path[P], dstUrl: Url[A])(implicit
  ev: B <:< FsObject): F[Int] =
    defaultTransferTo(this, dstStore, srcPath, dstUrl)

  override def getContents[A](path: Path[A], chunkSize: Int): F[String] =
    get(path, chunkSize).through(fs2.text.utf8Decode).compile.string
}

object SftpStore {

  /** Safely initialize SftpStore and disconnect Session upon finish.
    *
    * @param fSession
    *   – process used to connect to SFTP server, obtain Session.
    * @param maxChannels
    *   – optional upper limit on the number of concurrently open channels.
    * @param connectTimeout
    *   – override for channel connect timeout.
    * @return
    *   Resource[F, [SftpStore[F]], session open on start is going to be close in resource finalization.
    */
  def apply[F[_]: ConcurrentEffect: ContextShift](
    fSession: F[Session],
    blocker: Blocker,
    maxChannels: Option[Long] = None,
    connectTimeout: Int = 10000
  ): Resource[F, SftpStore[F]] =
    Resource.make(fSession.flatTap { session =>
      blocker.delay(session.connect()).recover {
        case e: JSchException if e.getMessage == "session is already connected" => ()
      }
    })(session => blocker.delay(session.disconnect())).flatMap { session =>
      Resource.eval(if (maxChannels.exists(_ < 1)) {
        new IllegalArgumentException(s"maxChannels must be >= 1").raiseError
      } else for {
        authority <- {
          val port = Option(session.getPort).filter(_ != 22).map(":" + _).getOrElse("")
          Authority.parse(session.getHost + port).leftMap(MultipleUrlValidationException.apply).liftTo[F]
        }
        semaphore <- maxChannels.traverse(Semaphore.apply[F])
        queue <- maxChannels match {
          case Some(max) => Queue.circularBuffer[F, ChannelSftp](max.toInt)
          case None      => Queue.unbounded[F, ChannelSftp]
        }
      } yield new SftpStore[F](authority, session, blocker, queue, semaphore, connectTimeout))
    }
}
