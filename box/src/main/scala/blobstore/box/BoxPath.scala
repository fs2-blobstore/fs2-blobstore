package blobstore
package box

import java.time.Instant

import cats.data.Chain
import com.box.sdk.{BoxFile, BoxFolder}

import scala.jdk.CollectionConverters._

class BoxPath private[box] (
  val root: Option[String],
  private[box] val rootId: Option[String],
  val fileOrFolder: Either[BoxFile#Info, BoxFolder#Info]
) extends Path {
  val pathFromRoot: Chain[String] = {
    val withRoot = fileOrFolder.fold(_.getPathCollection, _.getPathCollection).asScala
    val pathToSelf =
      Chain.fromSeq(
        withRoot.drop(withRoot.lastIndexWhere(info => rootId.contains(info.getID)) + 1).toIndexedSeq
      )
    fileOrFolder.fold(_ => pathToSelf, folder => pathToSelf.append(folder)).map(_.getName)
  }
  val fileName: Option[String]      = fileOrFolder.fold(file => Some(file.getName), _ => None)
  val size: Option[Long]            = fileOrFolder.fold(file => Option(file.getSize), _ => None)
  val isDir: Option[Boolean]        = Some(fileOrFolder.fold(_ => false, _ => true))
  val lastModified: Option[Instant] = Option(fileOrFolder.fold(_.getModifiedAt, _.getModifiedAt)).map(_.toInstant)
}

object BoxPath {
  def narrow(p: Path): Option[BoxPath] = p match {
    case bp: BoxPath => Some(bp)
    case _           => None
  }

  private[box] def rootFilePath(p: Path): List[String] = {
    val withRoot = p.root.fold(p.pathFromRoot)(p.pathFromRoot.prepend)
    p.fileName.fold(withRoot)(withRoot.append).filter(_.nonEmpty).toList
  }

  private[box] def parentPath(path: Path): List[String] =
    rootFilePath(path).dropRight(1)
}
