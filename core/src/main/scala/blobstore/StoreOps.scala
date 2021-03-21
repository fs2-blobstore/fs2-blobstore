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

import blobstore.url.{FsObject, Path, Url}
import cats.effect.{Blocker, ContextShift, Sync}
import cats.syntax.all._
import fs2.Pipe

/** This object contains shared implementations of functions that requires additional capabilities from the effect type
  */
class StoreOps[F[_]: Sync: ContextShift, B](store: Store[F, B]) {

  /** Write contents of src file into dst Path
    * @param src java.nio.file.Path
    * @param dst Path to write to
    * @return F[Unit]
    */
  def put(src: java.nio.file.Path, dst: Url, overwrite: Boolean, blocker: Blocker): F[Unit] =
    Sync[F].delay(Option(src.toFile.length)).map(_.filter(_ > 0)).flatMap { size =>
      fs2.io.file
        .readAll(src, blocker, 4096)
        .through(store.put(dst, overwrite, size))
        .compile
        .drain
    }

  /** Put sink that buffers all incoming bytes to local filesystem, computes buffered data size, then puts bytes
    * to store. Useful when uploading data to stores that require content size like S3Store.
    *
    * @param url Path to write to
    * @return Sink[F, Byte] buffered sink
    */
  def bufferedPut(url: Url, overwrite: Boolean, chunkSize: Int, blocker: Blocker): Pipe[F, Byte, Unit] =
    _.through(bufferToDisk[F](chunkSize, blocker)).flatMap {
      case (n, s) =>
        s.through(store.put(url, overwrite, Option(n)))
    }

  /** get src path and write to local file system
    * @param src Path to get
    * @param dst local file to write contents to
    * @return F[Unit]
    */
  def get(src: Url, dst: java.nio.file.Path, chunkSize: Int, blocker: Blocker): F[Unit] =
    store.get(src, chunkSize).through(fs2.io.file.writeAll[F](dst, blocker)).compile.drain

  /** getContents with default UTF8 decoder
    * @param url Path to get
    * @return F[String] with file contents
    */
  def getContents(url: Url, chunkSize: Int = 4096): F[String] = getContents(url, chunkSize, fs2.text.utf8Decode)

  /** Decode get bytes from path into a string using decoder and return concatenated string.
    *
    * USE WITH CARE, this loads all file contents into memory.
    *
    * @param url Path to get
    * @param decoder Pipe[F, Byte, String]
    * @return F[String] with file contents
    */
  def getContents(url: Url, chunkSize: Int, decoder: Pipe[F, Byte, String]): F[String] =
    store.get(url, chunkSize).through(decoder).compile.toList.map(_.mkString)

  /** Collect all list results in the same order as the original list Stream
    * @param url Path to list
    * @return F\[List\[Path\]\] with all items in the result
    */
  def listAll(url: Url): F[List[Path[B]]] =
    store.list(url).compile.toList

  /** Copy value of the given path in this store to the destination store.
    *
    * This is especially useful when transferring content into S3Store that requires to know content
    * size before starting content upload.
    *
    * This method will list items from srcPath, get the file size and put into dstStore with the given size. If
    * listing contents result in nested directories it will copy files inside dirs recursively.
    *
    * @param dstStore destination store
    * @param srcUrl path to transfer from (can be a path to a file or dir)
    * @param dstUrl path to transfer to (can be a path to a file or dir, if you are transferring multiple files,
    *                make sure that dstPath.isDir == true, otherwise all files will override destination.
    * @return Stream[F, Int] number of files transfered
    */
  def transferTo[BB](dstStore: Store[F, BB], srcUrl: Url, dstUrl: Url): F[Int] =
    store.list(srcUrl, recursive = true)
      .flatMap(p =>
        store.get(srcUrl.replacePath(p), 4096)
          .through(dstStore.put(dstUrl.copy(path = p.plain)))
          .last
          .map(_.fold(0)(_ => 1))
      )
      .fold(0)(_ + _)
      .compile.last.map(_.getOrElse(0))

  /** Remove all files from a store recursively, given a path
    */
  def removeAll(url: Url)(implicit ev: B <:< FsObject): F[Int] = {
    val isDir = store.stat(url).compile.last.map {
      case Some(d) => d.isDir
      case None    => url.path.show.endsWith("/")
    }

    isDir.flatMap { isDir =>
      store.list(url)
        .evalMap(p =>
          if (p.isDir) {
            removeAll(url / p.lastSegment)
          } else {
            val dp = if (isDir) url / p.lastSegment else url
            store.remove(dp, recursive = false).as(1)
          }
        )
        .compile
        .fold(0)(_ + _)
    }
  }

}
