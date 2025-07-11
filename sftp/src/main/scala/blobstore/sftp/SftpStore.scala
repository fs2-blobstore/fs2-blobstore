package blobstore.sftp

import blobstore.{defaultTransferTo, putRotateBase, PathStore, Store}
import blobstore.url.{Authority, FsObject, Path, Url}
import blobstore.url.Path.{AbsolutePath, Plain, RootlessPath}
import blobstore.url.exception.{MultipleUrlValidationException, Throwables}
import cats.data.{Validated, ValidatedNec}
import cats.effect.kernel.Async
import cats.effect.std.{Dispatcher, Queue, Semaphore}
import cats.effect.Resource
import cats.syntax.all.*
import com.jcraft.jsch.*
import fs2.{Pipe, Stream}

import java.io.OutputStream
import scala.util.Try

/** @param session
  *   connected jsch Session.
  * @param queue
  *   queue to hold channels to be reused.
  * @param semaphore
  *   optional semaphore to limit the number of concurrently open channels.
  * @param connectTimeoutMillis
  *   override for channel connect timeout.
  */
class SftpStore[F[_]: Async](
  session: Session,
  queue: Queue[F, ChannelSftp],
  semaphore: Option[Semaphore[F]],
  connectTimeoutMillis: Int
) extends PathStore[F, SftpFile] {

  private val getChannel: F[ChannelSftp] =
    queue.tryTake.flatMap {
      case Some(channel) => channel.pure[F]
      case None          =>
        val openF = Async[F].blocking {
          val ch = session.openChannel("sftp").asInstanceOf[ChannelSftp] // scalafix:ok
          ch.connect(connectTimeoutMillis)
          ch
        }
        semaphore.fold(openF) { s => s.tryAcquire.ifM(openF, getChannel) }
    }

  def closeChannel(ch: ChannelSftp): F[Unit] = semaphore match {
    case Some(s) => s.release.flatMap(_ => Async[F].blocking(ch.disconnect()))
    case None    => ().pure
  }

  private def channelResource: Resource[F, ChannelSftp] = Resource.make(getChannel) {
    case ch if ch.isClosed => ().pure[F]
    case ch                => queue.tryOffer(ch).ifM(().pure[F], closeChannel(ch))
  }

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[SftpFile]] = {

    def entrySelector(cb: ChannelSftp.LsEntry => Unit): ChannelSftp.LsEntrySelector = (entry: ChannelSftp.LsEntry) => {
      cb(entry)
      ChannelSftp.LsEntrySelector.CONTINUE
    }

    val stream = for {
      dispatcher <- Stream.resource(Dispatcher.sequential[F])
      q          <- Stream.eval(Queue.bounded[F, Option[ChannelSftp.LsEntry]](64))
      channel    <- Stream.resource(channelResource)
      entry      <- Stream.fromQueueNoneTerminated(q)
        .filter(e => e.getFilename != "." && e.getFilename != "..")
        .concurrently {
          // JSch hits index out of bounds on empty string, needs explicit handling
          val resolveEmptyPath = if (path.show.isEmpty) Async[F].blocking(channel.pwd()) else path.show.pure[F]
          val performList      = resolveEmptyPath.flatMap { path =>
            val es = entrySelector(e => dispatcher.unsafeRunSync(q.offer(Some(e))))
            Async[F].blocking(channel.ls(path.show, es)).attempt.flatMap(_ => q.offer(None))
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
        Async[F].blocking(channel.get(path.show)),
        chunkSize = chunkSize,
        closeAfterUse = true
      )
    }

  override def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = {
    in =>
      def pull(channel: ChannelSftp): Stream[F, Unit] =
        Stream
          .resource(outputStreamResource(channel, path, overwrite))
          .flatMap(os => in.through(fs2.io.writeOutputStream(os.pure, closeAfterUse = false)))

      Stream.resource(channelResource).flatMap(channel => pull(channel))
  }

  override def move[A, B](src: Path[A], dst: Path[B]): F[Unit] = channelResource.use { channel =>
    mkdirs(dst, channel) >> Async[F].blocking(channel.rename(src.show, dst.show))
  }

  override def copy[A, B](src: Path[A], dst: Path[B]): F[Unit] = channelResource.use { channel =>
    mkdirs(dst, channel) >> get(src, 4096).through(put(dst)).compile.drain
  }

  override def remove[A](path: Path[A], recursive: Boolean = false): F[Unit] = {
    def recursiveRemove(path: Path[SftpFile]): F[Unit] = channelResource.use { channel =>
      val r =
        if (path.isDir) {
          list(path).evalMap(recursiveRemove) ++ Stream.eval(Async[F].blocking(channel.rmdir(path.show)))
        } else Stream.eval(Async[F].blocking(channel.rm(path.show)))
      r.compile.drain
    }
    channelResource.use { channel =>
      _stat(path, channel).flatMap {
        case Some(p) =>
          if (recursive) recursiveRemove(p)
          else {
            if (p.isDir) Async[F].blocking(channel.rmdir(p.show)) else Async[F].blocking(channel.rm(p.show))
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

    putRotateBase(limit, openNewFile)(os => bytes => Async[F].blocking(os.write(bytes.toArray)))
  }

  private def mkdirs[A](path: Path[A], channel: ChannelSftp): F[Unit] = {
    val root: Path.Plain = path match {
      case AbsolutePath(_, _) => AbsolutePath.root
      case RootlessPath(_, _) => RootlessPath.root
    }

    fs2.Stream.emits(path.segments.toList)
      .covary[F]
      .evalScan(root) { (acc, el) =>
        Async[F].blocking(Try(channel.mkdir(acc.show))).as(acc / el)
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
        mkdirs(path, channel) >> Async[F].blocking(channel.put(path.show, ChannelSftp.OVERWRITE))
      if (overwrite) {
        newOrOverwrite
      } else {
        Async[F].blocking(channel.ls(path.show)).attempt.flatMap {
          case Left(e: SftpException) if e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE =>
            newOrOverwrite
          case Left(e)  => Async[F].raiseError(e)
          case Right(_) => Async[F].raiseError(new IllegalArgumentException(show"File at path '$path' already exist."))
        }
      }
    }

    def close(os: OutputStream): F[Unit] = Async[F].blocking(os.close())

    Resource.make(put(channel))(close)
  }

  override def stat[A](path: Path[A]): F[Option[Path[SftpFile]]] =
    channelResource.use { channel =>
      _stat(path, channel)
    }

  def _stat[A](path: Path[A], channel: ChannelSftp): F[Option[Path[SftpFile]]] =
    Async[F].blocking(channel.stat(path.show))
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
    get(path, chunkSize).through(fs2.text.utf8.decode).compile.string
}

object SftpStore {

  def resourceBuilder[F[_]: Async](mkSession: F[Session]): SftpStoreResourceBuilder[F] =
    SftpStoreResourceBuilderImpl[F](mkSession)

  /** @see
    *   [[SftpStore]]
    */
  trait SftpStoreResourceBuilder[F[_]] {
    def withMkSession(mkSession: F[Session]): SftpStoreResourceBuilder[F]
    def setMaxChannels(maybeMaxChannels: Option[Long]): SftpStoreResourceBuilder[F]
    def withConnectTimeout(connectTimeoutMillis: Long): SftpStoreResourceBuilder[F]
    def withMaxChannels(maxChannels: Long): SftpStoreResourceBuilder[F] = setMaxChannels(Some(maxChannels))
    def build: Resource[F, SftpStore[F]]
  }

  case class SftpStoreResourceBuilderImpl[F[_]: Async](
    _mkSession: F[Session],
    _maxChannels: Option[Long] = None,
    _connectTimeout: Long = 10000
  ) extends SftpStoreResourceBuilder[F] {
    def withMkSession(mkSession: F[Session]): SftpStoreResourceBuilder[F]           = this.copy(_mkSession = mkSession)
    def setMaxChannels(maybeMaxChannels: Option[Long]): SftpStoreResourceBuilder[F] =
      this.copy(_maxChannels = maybeMaxChannels)
    def withConnectTimeout(connectTimeoutMillis: Long): SftpStoreResourceBuilder[F] =
      this.copy(_connectTimeout = connectTimeoutMillis)
    def build: Resource[F, SftpStore[F]] = {
      val validateConnectTimeout: ValidatedNec[Throwable, Unit] =
        if (_connectTimeout < 100) {
          new IllegalArgumentException("Please set connectTimeout to be at least 100ms.").invalidNec
        } else if (_connectTimeout > Int.MaxValue.toLong) {
          new IllegalArgumentException(show"connectTimeout cannot exceed ${Int.MaxValue}.").invalidNec
        } else ().validNec
      val validateMaxChannels: ValidatedNec[Throwable, Unit] = _maxChannels match {
        case Some(maxC) if maxC < 1 =>
          new IllegalArgumentException("Please set maxChannels to be at least 1.").invalidNec
        case _ => ().validNec
      }

      List(validateConnectTimeout, validateMaxChannels).combineAll match {
        case Validated.Valid(_)    => sessionResource.evalMap(init)
        case Validated.Invalid(es) =>
          Resource.raiseError[F, SftpStore[F], Throwable](es.reduce(Throwables.collapsingSemigroup))
      }
    }

    private def sessionResource = Resource.make {
      _mkSession.flatTap { session =>
        Async[F].blocking(session.connect()).recover {
          case e: JSchException if e.getMessage == "session is already connected" => ()
        }
      }
    } { session =>
      Async[F].blocking(session.disconnect())
    }

    private def init(session: Session): F[SftpStore[F]] = {
      val port       = Option(session.getPort).filter(_ != 22).map(":" + _).getOrElse("")
      val _authority = Authority.parse(session.getHost + port).leftMap(MultipleUrlValidationException.apply)
      for {
        _         <- Async[F].fromValidated(_authority)
        semaphore <- _maxChannels.traverse(Semaphore.apply[F])
        queue     <- _maxChannels match {
          case Some(max) => Queue.circularBuffer[F, ChannelSftp](max.toInt)
          case None      => Queue.unbounded[F, ChannelSftp]
        }
      } yield new SftpStore[F](session, queue, semaphore, _connectTimeout.toInt)
    }

  }
}
