package blobstore.url

import java.time.Instant

import blobstore.url.Path.{AbsolutePath, RootlessPath}
import cats.{Functor, Show}
import cats.data.Chain
import cats.instances.int._
import cats.instances.string._
import cats.kernel.{Eq, Order}
import cats.syntax.all._

/**
  * The path segment of a URI. It is parameterized on the type representing the path. This can be a plain String, or a
  * storage provider specific type.
  *
  * Examples of storage provider types would be `software.amazon.awssdk.services.s3.internal.resource.S3ObjectResource`
  * for S3, com.google.storage.Blob for GCS, etc.
  *
  * @see https://www.ietf.org/rfc/rfc3986.txt chapter 3.3, Path
  */
sealed trait Path[A] {
  def representation: A
  def segments: Chain[String]
  def fileName: Option[String] = segments.lastOption.filter(l => !l.endsWith("/"))

  def plain: Path.Plain = this match {
    case AbsolutePath(_, segments) => AbsolutePath("/" + segments.mkString_("/"), segments)
    case RootlessPath(_, segments) => RootlessPath(segments.mkString_(""), segments)
  }

  def /(segment: String)(implicit ev: String =:= A): Path[A] = this match {
    case AbsolutePath(_, s) =>
      val c = s :+ segment
      AbsolutePath("/" + ev(c.mkString_("/")), c)
    case RootlessPath(_, segments) =>
      val s = segments :+ segment
      if (segments.isEmpty) RootlessPath(segment, s)
      else RootlessPath(s.mkString_("/"), s)
  }

  def as[B](b: B): Path[B] = this match {
    case AbsolutePath(_, segments) => AbsolutePath(b, segments)
    case RootlessPath(_, segments) => RootlessPath(b, segments)
  }

  def addSegment[B](segment: String, representation: B): Path[B] = this match {
    case AbsolutePath(_, _) => AbsolutePath(representation, segments :+ segment)
    case RootlessPath(_, _) => RootlessPath(representation, segments :+ segment)
  }

  def size(implicit B: FsObject[A]): Long                    = FsObject[A].size(representation)
  def isDir(implicit B: FsObject[A]): Boolean                = FsObject[A].isDir(representation)
  def lastModified(implicit B: FsObject[A]): Option[Instant] = FsObject[A].lastModified(representation)
}

object Path {

  /**
    * A plain path represented with a String
    *
    * See .apply for creating these
    */
  type Plain = Path[String]

  case class AbsolutePath[A](representation: A, segments: Chain[String]) extends Path[A]

  object AbsolutePath {
    def parse(s: String): Option[AbsolutePath[String]] =
      if (s.startsWith("/")) Some(AbsolutePath(s, Chain(s.split("/").toList: _*)))
      else None

    val root: AbsolutePath[String] = AbsolutePath("/", Chain.empty)

    implicit def show[A]: Show[AbsolutePath[A]]          = "/" + _.segments.mkString_("/")
    implicit def order[A: Order]: Order[AbsolutePath[A]] = (x, y) => Order[A].compare(x.representation, y.representation)
  }

  case class RootlessPath[A](representation: A, segments: Chain[String]) extends Path[A]
  object RootlessPath {
    def parse(s: String): Option[RootlessPath[String]] =
      if (s.nonEmpty && !s.startsWith("/")) Some(RootlessPath(s, Chain(s.split("/").toList: _*)))
      else None

    def relativeHome: RootlessPath[String] = RootlessPath("", Chain.empty)

    implicit def show[A]: Show[RootlessPath[A]]          = _.segments.mkString_("/")
    implicit def order[A: Order]: Order[RootlessPath[A]] = (x, y) => Order[A].compare(x.representation, y.representation)
    implicit def ordering[A: Ordering]: Ordering[RootlessPath[A]] = order[A](Order.fromOrdering[A]).toOrdering
  }

  def empty: RootlessPath[String] = RootlessPath("", Chain.empty)

  def apply(s: String): Path.Plain =
    AbsolutePath.parse(s).orElse(RootlessPath.parse(s)).getOrElse(
      RootlessPath(s, Chain(s.split("/").toList: _*)) // Paths either have a root or they don't, this block is never executed
    )

  def compare[A](one: Path[A], two: Path[A]): Int = {
    one.show compare two.show
  }

  implicit def order[A: Order]: Order[Path[A]] = compare
  implicit def ordering[A: Ordering]: Ordering[Path[A]] = order[A](Order.fromOrdering[A]).toOrdering

  implicit def eq[A]: Eq[Path[A]] = compare[A](_, _) === 0

  implicit def show[A]: Show[Path[A]] = {
    case a: AbsolutePath[A] => a.show
    case r: RootlessPath[A] => r.show
  }

  implicit val functor: Functor[Path] = new Functor[Path] {
    override def map[A, B](fa: Path[A])(f: A => B): Path[B] = fa match {
      case _: AbsolutePath[A] => AbsolutePath[B](f(fa.representation), fa.segments)
      case p: RootlessPath[A] => RootlessPath(f(p.representation), p.segments)
    }
  }

}
