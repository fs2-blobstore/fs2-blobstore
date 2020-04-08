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

import fs2.{Pipe, Stream}

trait Store[F[_]] {

  /**
    * List paths. See [[StoreOps.ListOps]] for convenient listAll method.
    *
    * @param path to list
    * @param recursive when true returned list would contain files at given path and all sub-folders but no folders,
    *                  otherwise – return files and folder at given path.
    * @return stream of Paths. Implementing stores must guarantee that returned Paths
    *         have correct values for size, isDir and lastModified.
    * @example Given Path pointing at folder:
    *          folder/a
    *          folder/b
    *          folder/c
    *          folder/sub-folder/d
    *          folder/sub-folder/sub-sub-folder/e
    *
    *          list(folder, recursive = true)  -> [a, b, c, d, e]
    *          list(folder, recursive = false) -> [a, b, c, sub-folder]
    */
  def list(path: Path, recursive: Boolean = false): Stream[F, Path]

  /**
    * Get bytes for the given Path. See [[StoreOps.GetOps]] for convenient get and getContents methods.
    * @param path to get
    * @param chunkSize bytes to read in each chunk.
    * @return stream of bytes
    */
  def get(path: Path, chunkSize: Int): Stream[F, Byte]

  /**
    * Provides a Sink that writes bytes into the provided path. See [[StoreOps.PutOps]] for convenient put String
    * and put file methods.
    *
    * It is highly recommended to provide [[Path.size]] when writing as it allows for optimizations in some store.
    * Specifically, S3Store will behave very poorly if no size is provided as it will load all bytes in memory before
    * writing content to S3 server.
    *
    * @param path to put
    * @param overwrite when true putting to path with pre-existing file would overwrite the content, otherwise – fail with error.
    * @return sink of bytes
    */
  def put(path: Path, overwrite: Boolean = true): Pipe[F, Byte, Unit]

  /**
    * Moves bytes from srcPath to dstPath. Stores should optimize to use native move functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def move(src: Path, dst: Path): F[Unit]

  /**
    * Copies bytes from srcPath to dstPath. Stores should optimize to use native copy functions to avoid data transfer.
    * @param src path
    * @param dst path
    * @return F[Unit]
    */
  def copy(src: Path, dst: Path): F[Unit]

  /**
    * Remove bytes for given path. Call should succeed even if there is nothing stored at that path.
    * @param path to remove
    * @return F[Unit]
    */
  def remove(path: Path): F[Unit]

  /**
    * Writes all data to a sequence of blobs/files, each limited in size to `limit`.
    *
    * The `computePath` operation is used to compute the path of the first file
    * and every subsequent file. Typically, the next file should be determined
    * by analyzing the current state of the filesystem -- e.g., by looking at all
    * files in a directory and generating a unique name.
    *
    * @note Put of all files uses overwrite semantic, i.e. if path returned by [[computePath]] already exists content will be overwritten.
    *       If that doesn't suit your use case use [[computePath]] to guard against overwriting existing files.
    *
    * @param computePath operation to compute the path of the first file and all subsequent files.
    * @param limit maximum size in bytes for each file.
    * @return sink of bytes
    */
  def putRotate(computePath: F[Path], limit: Long): Pipe[F, Byte, Unit]

}
