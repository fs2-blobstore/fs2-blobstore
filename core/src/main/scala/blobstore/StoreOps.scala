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

import blobstore.url.{FsObject, Url}
import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.Pipe
import fs2.io.file.{Files, Path}

import java.nio.charset.StandardCharsets

/** This object contains shared implementations of functions that requires additional capabilities from the effect type
  */
class StoreOps[F[_]: Files: Concurrent, B](store: Store[F, B]) {

  /** Write contents of src file into dst Path
    * @param src
    *   java.nio.file.Path
    * @param dst
    *   Path to write to
    * @return
    *   F[Unit]
    */
  def putFromNio[A](src: java.nio.file.Path, dst: Url[A], overwrite: Boolean): F[Unit] = {
    val fs2Path = Path.fromNioPath(src)
    val put = Files[F].size(fs2Path).flatMap {
      case size if size > 0 =>
        Files[F]
          .readAll(fs2Path)
          .through(store.put(dst, overwrite, size.some))
          .compile
          .drain
      case _ =>
        ().pure
    }
    Files[F].isRegularFile(fs2Path).ifM(put, new Exception("Not a file").raiseError)
  }

  /** Put sink that buffers all incoming bytes to local filesystem, computes buffered data size, then puts bytes to
    * store. Useful when uploading data to stores that require content size like S3Store.
    *
    * @param url
    *   Path to write to
    * @return
    *   Sink[F, Byte] buffered sink
    */
  def bufferedPut[A](url: Url[A], overwrite: Boolean, chunkSize: Int): Pipe[F, Byte, Unit] =
    _.through(bufferToDisk[F](chunkSize)).flatMap {
      case (n, s) =>
        s.through(store.put(url, overwrite, Option(n)))
    }

  /** get src path and write to local file system
    * @param src
    *   Path to get
    * @param dst
    *   local file to write contents to
    * @return
    *   F[Unit]
    */
  def getToNio[A](src: Url[A], dst: java.nio.file.Path, chunkSize: Int): F[Unit] =
    store.get(src, chunkSize).through(Files[F].writeAll(Path.fromNioPath(dst))).compile.drain

  def putContent[A](url: Url[A], content: String): F[Unit] = {
    val bytes = content.getBytes(StandardCharsets.UTF_8)
    fs2.Stream
      .emits(bytes)
      .covary[F]
      .through(store.put(url, size = Some(bytes.length.toLong)))
      .compile.drain
  }

  /** getContents with default UTF8 decoder
    * @param url
    *   Url to get
    * @return
    *   F[String] with file contents
    */
  def getContents[A](url: Url[A], chunkSize: Int = 4096): F[String] = getContents(url, chunkSize, fs2.text.utf8.decode)

  /** Decode get bytes from path into a string using decoder and return concatenated string.
    *
    * USE WITH CARE, this loads all file contents into memory.
    *
    * @param url
    *   Path to get
    * @param decoder
    *   Pipe[F, Byte, String]
    * @return
    *   F[String] with file contents
    */
  def getContents[A](url: Url[A], chunkSize: Int, decoder: Pipe[F, Byte, String]): F[String] =
    store.get(url, chunkSize).through(decoder).compile.toList.map(_.mkString)

  /** Collect all list results in the same order as the original list Stream
    * @param url
    *   Url to list
    * @return
    *   F\[List\[Url\]\] with all items in the result
    */
  def listAll[A](url: Url[A], recursive: Boolean = false): F[List[Url[B]]] =
    store.list(url, recursive).compile.toList

  /** Copy value of the given path in this store to the destination store.
    *
    * This method will list item at srcUrl and copy it to dstUrl in dstStore. If srcUrl points to a directory, it will
    * copy files inside recursively.
    *
    * @param dstStore
    *   destination store
    * @param srcUrl
    *   url to transfer from (can be a path to a file or directory)
    * @param dstUrl
    *   url to transfer to (if srcUrl points to a directory, interpreted as a prefix)
    * @return
    *   F[Int] number of files transferred
    */
  def transferTo[BB, A, C](dstStore: Store[F, BB], srcUrl: Url[A], dstUrl: Url[C]): F[Int] =
    store.list(srcUrl, recursive = true).evalMap { u =>
      val targetUrl =
        if (u.path.plain === srcUrl.path.plain) dstUrl.plain
        else dstUrl / srcUrl.path.nioPath.relativize(u.path.nioPath).toString
      store.get(u, 4096).through(dstStore.put(targetUrl)).compile.drain.as(1)
    }.compile.fold(0)(_ + _)

  /** Remove all files from a store recursively, given a path
    */
  def removeAll[A](url: Url[A])(implicit ev: B <:< FsObject): F[Int] = {
    val isDir = store.stat(url).compile.last.map {
      case Some(d) => d.path.isDir
      case None    => url.path.show.endsWith("/")
    }

    isDir.flatMap { isDir =>
      store.list(url)
        .evalMap(u =>
          if (u.path.isDir) {
            removeAll(url / u.path.lastSegment)
          } else {
            val dUrl: Url.Plain = if (isDir) url / u.path.lastSegment else url.withPath(url.path.plain)
            store.remove(dUrl, recursive = false).as(1)
          }
        )
        .compile
        .fold(0)(_ + _)
    }
  }

}
