package blobstore

import cats.syntax.foldable._
import cats.instances.string._

trait PathOps {
  implicit class AppendOps(path: Path) {

    /**
      * Concatenate string to end of path separated by Path.SEP
      * @param str String to append
      * @return concatenated Path
      */
    def /(str: String): Path = {
      val separator = if (str.headOption.contains('/')) "" else "/"
      val pathStr =
        (path.root.fold("") { _ ++ "/" } ++ path.pathFromRoot.mkString_("", "/", path.fileName.fold("")("/" ++ _))).trim
      Path(if (pathStr.isEmpty) str else s"$pathStr$separator$str")
    }

    /**
      * Concatenate other Path to end of this path separated by Path.SEP,
      * this method ignores the other path root in favor of this path root.
      * @param other Path to append
      * @return concatenated Path
      */
    def /(other: Path): Path = /(other.pathFromRoot.mkString_("", "/", other.fileName.fold("")("/" + _)))

    /**
      * Returns last segment no matter if path points to directory or file.
      * @return Last segment
      */
    def lastSegment: String =
      path.fileName.getOrElse(
        path.pathFromRoot.lastOption.orElse(path.root.flatMap(_.split('/').lastOption)).fold("/")(_ + "/")
      )

    def filePath: String = path.fileName.fold(path.pathFromRoot)(path.pathFromRoot.append).mkString_("/")
  }

}

object PathOps extends PathOps
