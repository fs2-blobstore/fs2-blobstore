package blobstore.experiment.url

import java.time.Instant

import cats.data.Chain
import cats.instances.string._
import cats.instances.int._
import cats.syntax.all._
import cats.Show
import cats.kernel.{Eq, Order}

/**
  * https://www.ietf.org/rfc/rfc3986.txt chapter 3.3, Path
  */
sealed trait Path[+A] {
  def info: A
  def segments: Chain[String]
  def fileName: Option[String] = segments.lastOption.filter(l => !l.endsWith("/"))

  def size(implicit B: Blob[A]): Long = Blob[A].size(info)
  def isDir(implicit B: Blob[A]): Boolean = Blob[A].isDir(info)
  def lastModified(implicit B: Blob[A]): Option[Instant] = Blob[A].lastModified(info)
}

object Path {

  /**
    * A plain path represented with a String
    *
    * See .apply for creating these
    */
  type Plain = Path[String]

  case class AbsolutePath[A](info: A, segments: Chain[String]) extends Path[A]

  object AbsolutePath {
    def unapply(s: String): Option[AbsolutePath[String]] =
      if (s.startsWith("/")) Some(AbsolutePath(s, Chain(s.split("/").toList: _*)))
      else None

    implicit def show[A]: Show[AbsolutePath[A]] = "/" + _.segments.mkString_("/")
    implicit def order[A: Order]: Order[AbsolutePath[A]] = (x, y) => Order[A].compare(x.info, y.info)
  }

  case class RootlessPath[A](info: A, segments: Chain[String]) extends Path[A]
  object RootlessPath {
    def unapply(s: String): Option[RootlessPath[String]] =
      if (s.nonEmpty && !s.startsWith("/")) Some(RootlessPath(s, Chain(s.split("/").toList: _*)))
      else None

    implicit def show[A]: Show[RootlessPath[A]] = _.segments.mkString_("/")
    implicit def order[A: Order]: Order[RootlessPath[A]] = (x, y) => Order[A].compare(x.info, y.info)
  }

  object EmptyPath extends Path[String] {
    def unapply(s: String): Option[Path.Plain] =
      if (s.isEmpty) Some(EmptyPath) else None

    val info: String = ""
    val segments: Chain[String] = Chain.empty

    implicit val show: Show[EmptyPath.type] = _ => ""
    implicit val order: Order[EmptyPath.type] = (_, _) => 0
  }

  def apply(s: String): Path.Plain = s match {
    case AbsolutePath(p) => p
    case RootlessPath(p) => p
    case EmptyPath(p)    => p

    // Convincing the compiler that all Strings are in fact paths
    case _  => RootlessPath(s, Chain(s.split("/").toList: _*))
  }

  def compare[A](one: Path[A], two: Path[A]): Int = {
    one.show compare two.show
  }

  implicit def order[A: Order]: Order[Path[A]] = compare

  implicit def eq[A]: Eq[Path[A]] = compare[A](_,_) === 0

  implicit def show[A]: Show[Path[A]] = {
    case a: AbsolutePath[A] => a.show
    case r: RootlessPath[A] => r.show
    case _: EmptyPath.type => ""
  }


}
