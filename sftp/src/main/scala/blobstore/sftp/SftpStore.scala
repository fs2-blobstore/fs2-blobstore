/*
Copyright 2018 LendUp Global, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package blobstore
package sftp

import com.jcraft.jsch._
import cats.syntax.all._
import cats.instances.option._
import cats.instances.string._
import java.io.OutputStream

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Effect, IO, Resource}
import cats.effect.concurrent.{MVar, Semaphore}
import fs2.{Pipe, Stream}
import fs2.concurrent.Queue
import implicits._

import scala.util.Try

final class SftpStore[F[_]](
  absRoot: String,
  private[sftp] val session: Session,
  blocker: Blocker,
  mVar: MVar[F, ChannelSftp],
  semaphore: Option[Semaphore[F]],
  connectTimeout: Int
)(implicit F: ConcurrentEffect[F], CS: ContextShift[F])
  extends Store[F] {

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

  /**
    * @param path path to list
    * @return Stream[F, Path]
    */
  override def list(path: Path): Stream[F, SftpPath] = {

    def entrySelector(cb: ChannelSftp#LsEntry => Unit): ChannelSftp.LsEntrySelector = (entry: ChannelSftp#LsEntry) => {
      cb(entry)
      ChannelSftp.LsEntrySelector.CONTINUE
    }

    for {
      q       <- Stream.eval(Queue.bounded[F, Option[ChannelSftp#LsEntry]](64))
      channel <- Stream.resource(channelResource)
      entry <- q.dequeue.unNoneTerminate
        .filter(e => e.getFilename != "." && e.getFilename != "..")
        .concurrently(
          Stream.eval {
            val strPath = SftpStore.strPath(absRoot, path).stripSuffix("/")
            val es      = entrySelector(e => Effect[F].runAsync(q.enqueue1(Some(e)))(_ => IO.unit).unsafeRunSync())
            blocker.delay(channel.ls(strPath, es)).attempt.flatMap(_ => q.enqueue1(None))
          }
        )
    } yield {
      val isDir = Option(entry.getAttrs.isDir)
      val newPath =
        if (path.fileName.contains(entry.getFilename)) path
        else path / (entry.getFilename ++ (if (isDir.contains(true)) "/" else ""))

      SftpPath(newPath.root, newPath.fileName, newPath.pathFromRoot, entry.getAttrs)
    }
  }

  override def get(path: Path, chunkSize: Int): Stream[F, Byte] =
    Stream.resource(channelResource).flatMap { channel =>
      fs2.io.readInputStream(
        blocker.delay(channel.get(SftpStore.strPath(absRoot, path))),
        chunkSize = chunkSize,
        closeAfterUse = true,
        blocker = blocker
      )
    }

  override def put(path: Path, overwrite: Boolean = true): Pipe[F, Byte, Unit] = { in =>
    def put(channel: ChannelSftp): F[OutputStream] = {
      val newOrOverwrite =
        mkdirs(path, channel) *> blocker.delay(channel.put(SftpStore.strPath(absRoot, path), ChannelSftp.OVERWRITE))
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

    def pull(channel: ChannelSftp): Stream[F, Unit] =
      Stream
        .bracket(put(channel))(close)
        .flatMap(os => _writeAllToOutputStream1(in, os, blocker).stream)

    Stream.resource(channelResource).flatMap(channel => pull(channel))
  }

  override def move(src: Path, dst: Path): F[Unit] = channelResource.use { channel =>
    mkdirs(dst, channel) *> blocker.delay(
      channel.rename(SftpStore.strPath(absRoot, src), SftpStore.strPath(absRoot, dst))
    )
  }

  override def copy(src: Path, dst: Path): F[Unit] = {
    val s = for {
      channel <- Stream.resource(channelResource)
      _       <- Stream.eval(mkdirs(dst, channel))
      _       <- get(src).through(this.bufferedPut(dst, blocker))
    } yield ()

    s.compile.drain
  }

  override def remove(path: Path): F[Unit] = channelResource.use { channel =>
    blocker.delay {
      try {
        val strPath = SftpStore.strPath(absRoot, path)
        if (path.isDir.getOrElse(path.fileName.isEmpty)) channel.rmdir(strPath) else channel.rm(strPath)
      } catch {
        // Let the remove() call succeed if there is no file at this path
        case e: SftpException if e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE => ()
      }
    }
  }

  private def mkdirs(path: Path, channel: ChannelSftp): F[Unit] =
    blocker.delay {
      val pathString = SftpStore.strPath(absRoot, path)
      val root       = if (pathString.startsWith("/")) "/" else ""

      pathString.split('/').foldLeft(root) {
        case (acc, s) =>
          Try(channel.mkdir(acc))
          val candidate = List(acc, s).filter(_.nonEmpty).mkString("/")
          // Don't .replaceAll("//"). Directories containing "//" are valid according to spec
          if (root == "/" && candidate.startsWith("//")) candidate.replaceFirst("//", "/")
          else candidate
      }
      ()
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
    absRoot: String,
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

  private def strPath(absRoot: String, path: Path): String = {
    val withName = path.fileName.fold(path.pathFromRoot)(path.pathFromRoot.append)
    val withRoot = path.root.fold(withName)(withName.prepend)
    val str      = withRoot.prepend(absRoot).filter(_.nonEmpty).mkString_("/")
    str ++ (if (path.fileName.isEmpty) "/" else "")
  }
}
