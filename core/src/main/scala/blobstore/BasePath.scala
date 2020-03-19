package blobstore

import java.net.URI
import java.time.Instant

import cats.data.Chain

class BasePath private[blobstore] (
  val root: Option[String],
  val pathFromRoot: Chain[String],
  val fileName: Option[String],
  val size: Option[Long],
  val isDir: Option[Boolean],
  val lastModified: Option[Instant]
) extends Path

object BasePath {
  val empty: BasePath = BasePath(None, Chain.empty, None, None, Some(true), None)

  def apply(
    root: Option[String],
    pathFromRoot: Chain[String],
    fileName: Option[String],
    size: Option[Long],
    isDir: Option[Boolean],
    lastModified: Option[Instant]
  ): BasePath = new BasePath(root, pathFromRoot, fileName, size, isDir, lastModified)
  def fromString(s: String, forceRoot: Boolean): Option[Path] =
    if (s.isEmpty) Some(empty)
    else
      scala.util.Try(URI.create(s)).toOption.flatMap { uri =>
        val maybePath = Option(uri.getPath)
        val pathSegments =
          maybePath.map(_.split("/").filterNot(_.isEmpty)).getOrElse(Array.empty)
        val authority = Option(uri.getAuthority)
        val root =
          authority.orElse(if (forceRoot) pathSegments.headOption else None)
        if ((root.isEmpty && forceRoot) || root.exists(_.isEmpty))
          Option.empty[Path]
        else {
          val (pathFromRoot, fileName) = {
            val ps = if (root.isDefined && authority.isEmpty) pathSegments.drop(1) else pathSegments
            val fn = if (maybePath.exists(_.endsWith("/"))) None else ps.lastOption
            val ss = fn.fold(ps)(_ => ps.init)
            Chain.fromSeq(ss.toIndexedSeq) -> fn
          }
          Some(
            BasePath(root, pathFromRoot, fileName, None, None, None)
          )
        }
      }
}
