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

import java.time.Instant

import cats.data.Chain
import cats.syntax.foldable._
import cats.instances.string._

/**
  * Open trait for path abstraction.
  */
trait Path {

  /**
    * Returns optional root of this path.
    * It might be bucket name or specific directory to which it is necessary to restrict access to.
    *
    * @example Given path pointing inside S3 bucket 'bucket':
    *          val p: Path = ???
    *          p.root == Some("bucket")
    *
    */
  def root: Option[String]

  /**
    * Returns names of directories along the path to the item, starting at the root.
    *
    * @example Given path pointing to a local file in /root/dir1/dir2/file with root == "root":
    *          val p: Path = ???
    *          p.pathFromRoot == List("dir1", "dir2")
    *
    * @example Given path pointing to a local directory in /root/dir1/dir2/dir/ with root == "root":
    *          val p: Path = ???
    *          p.pathFromRoot == List("dir1", "dir2", "dir")
    */
  def pathFromRoot: Chain[String]

  /**
    * Returns name of the file pointed by this path.
    *
    * @example Given path pointing to a local file in /root/dir1/dir2/file with root == "root":
    *          val p: Path = ???
    *          p.fileName == Some("file")
    *
    * @note Some blob stores (s3, gcs) having flat namespaces allow trailing slashes in file names.
    *       @example File 'gs://bucket/this/is/flat/name/' in underlying storage has flat name "this/is/flat/name/".
    *                This would be represented as Path:
    *                val p: Path = ???
    *                p.pathFromRoot == List("this", "is", "flat", "name")
    *                p.fileName == None
    *                p.isDir == Some(true)
    *       @see [[isDir]]
    */
  def fileName: Option[String]

  /**
    * Returns size in bytes of the file pointed by this path if known. For directories always return None.
    *
    * @example Given path pointing to a local 20 byte file:
    *          val p: Path = ???
    *          p.size = Some(20)
    *
    */
  def size: Option[Long]

  /**
    * If known returns true if path points to a directory, otherwise – false.
    * It's not always possible to know from string representation whether path points to directory or file ending with trailing slash.
    * Paths returned by [[blobstore.Store.list]] must have this field defined.
    *
    * @example Given path pointing to a local file in /root/dir1/dir2/file with root == "root":
    *          val p: Path = ???
    *          p.isDir == Some(false)
    * @example Given path pointing to a local directory in /root/dir1/dir2/dir/ with root == "root":
    *          val p: Path = ???
    *          p.isDir == Some(true)
    * @example Given path pointing to object in S3 bucket 's3://bucket/this/is/flat/name/':
    *          val p: Path = ???
    *          p.isDire = Some(false)
    * @example Given path created from String:
    *          val p: Path = ???
    *          p.isDir == None
    */
  def isDir: Option[Boolean]

  /**
    * Returns most recent time when file pointed by this path has been modified if known.
    */
  def lastModified: Option[Instant]

  /**
    * Returns path with [[root]] field set to provided value.
    * @param newRoot - value to be set as [[root]].
    * @param reset - when true fields [[isDir]], [[size]] and [[lastModified]] in returned object would be set to None, otherwise kept the same.
    * @return new Path with [[root]] field set to provided param.
    */
  def withRoot(newRoot: Option[String], reset: Boolean = true): Path =
    if (root == newRoot) this
    else
      BasePath(
        newRoot,
        pathFromRoot,
        fileName,
        if (reset) None else size,
        if (reset) None else isDir,
        if (reset) None else lastModified
      )

  /**
    * Returns path with [[pathFromRoot]] field set to provided value.
    * @param newPathFromRoot - value to be set as [[pathFromRoot]].
    * @param reset - when true fields [[isDir]], [[size]] and [[lastModified]] in returned object would be set to None, otherwise kept the same.
    * @return new Path with [[pathFromRoot]] field set to provided param.
    */
  def withPathFromRoot(newPathFromRoot: Chain[String], reset: Boolean = true): Path =
    if (newPathFromRoot == pathFromRoot) this
    else
      BasePath(
        root,
        newPathFromRoot,
        fileName,
        if (reset) None else size,
        if (reset) None else isDir,
        if (reset) None else lastModified
      )

  /**
    * Returns path with [[fileName]] field set to provided value.
    * @param newFileName - value to be set as [[fileName]].
    * @param reset - when true fields [[isDir]], [[size]] and [[lastModified]] in returned object would be set to None, otherwise kept the same.
    * @return new Path with [[fileName]] field set to provided param.
    */
  def withFileName(newFileName: Option[String], reset: Boolean = true): Path =
    if (newFileName == fileName) this
    else
      BasePath(
        root,
        pathFromRoot,
        newFileName,
        if (reset) None else size,
        if (reset) None else isDir,
        if (reset) None else lastModified
      )

  /**
    * Returns path with [[size]] field set to provided value.
    * @param newSize - value to be set as [[size]].
    * @param reset - when true fields [[isDir]] and [[lastModified]] in returned object would be set to None, otherwise kept the same.
    * @return new Path with [[size]] field set to provided param.
    */
  def withSize(newSize: Option[Long], reset: Boolean = true): Path =
    if (newSize == size) this
    else BasePath(root, pathFromRoot, fileName, newSize, if (reset) None else isDir, if (reset) None else lastModified)

  /**
    * Returns path with [[isDir]] field set to provided value.
    * @param newIsDir - value to be set as [[isDir]].
    * @param reset - when true fields [[size]] and [[lastModified]] in returned object would be set to None, otherwise kept the same.
    * @return new Path with [[isDir]] field set to provided param.
    */
  def withIsDir(newIsDir: Option[Boolean], reset: Boolean = true): Path =
    if (newIsDir == isDir) this
    else BasePath(root, pathFromRoot, fileName, if (reset) None else size, newIsDir, if (reset) None else lastModified)

  /**
    * Returns path with [[lastModified]] field set to provided value.
    * @param newLastModified - value to be set as [[lastModified]].
    * @param reset - when true fields [[size]] and [[isDir]] in returned object would be set to None, otherwise kept the same.
    * @return new Path with [[lastModified]] field set to provided param.
    */
  def withLastModified(newLastModified: Option[Instant], reset: Boolean = true): Path =
    if (newLastModified == lastModified) this
    else BasePath(root, pathFromRoot, fileName, if (reset) None else size, if (reset) None else isDir, newLastModified)

  @SuppressWarnings(Array("scalafix:Disable.Any"))
  override def equals(obj: Any): Boolean = obj match {
    case p: Path =>
      root == p.root &&
        pathFromRoot == p.pathFromRoot &&
        fileName == p.fileName &&
        size == p.size &&
        isDir == p.isDir &&
        lastModified == p.lastModified
    case _ => false
  }

  override def toString: String =
    root.fold(pathFromRoot)(pathFromRoot.prepend).mkString_("", "/", fileName.fold("/")("/" + _))
}

object Path {
  def apply(
    r: Option[String],
    pfr: Chain[String],
    fn: Option[String],
    s: Option[Long],
    id: Option[Boolean],
    lm: Option[Instant]
  ): Path =
    BasePath(r, pfr, fn, s, id, lm)

  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  def apply(s: String): Path =
    fromString(s).getOrElse(throw new IllegalArgumentException(s"Unable to extract path from '$s'"))

  def fromString(s: String): Option[Path] = fromString(s, forceRoot = true)

  def fromString(s: String, forceRoot: Boolean): Option[Path] = BasePath.fromString(s, forceRoot)

  def unapply(
    p: Path
  ): Option[(Option[String], Chain[String], Option[String], Option[Long], Option[Boolean], Option[Instant])] =
    Some(
      (
        p.root,
        p.pathFromRoot,
        p.fileName,
        p.size,
        p.isDir,
        p.lastModified
      )
    )
}
