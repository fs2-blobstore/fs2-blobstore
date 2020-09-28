package blobstore.url

import java.nio.file.Paths
import java.time.Instant

import blobstore.url.Path.{AbsolutePath, RootlessPath}
import blobstore.url.general.UniversalFileSystemObject
import cats.{Eq, Functor, Show}
import cats.data.{Chain, NonEmptyChain}
import cats.instances.string._
import cats.instances.list._
import cats.kernel.Order
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
  def value: String = Show[Path[A]].show(this)

  def plain: Path.Plain = this match {
    case AbsolutePath(_, segments) => AbsolutePath("/" + segments.mkString_("/"), segments)
    case RootlessPath(_, segments) => RootlessPath(segments.mkString_(""), segments)
  }

  def absolute: AbsolutePath[String] = plain match {
    case p@AbsolutePath(_, _) => p
    case RootlessPath(a, segments) => AbsolutePath("/" + a, segments)
  }

  def relative: RootlessPath[String] = plain match {
    case AbsolutePath(a, segments) => RootlessPath(a.stripPrefix("/"), segments)
    case p@RootlessPath(a, segments) => p
  }

  def /(segment: String)(implicit ev: String =:= A): Path[String] = {
    val nonEmpty = Chain(segment.stripPrefix("/").split("/").toList: _*)
    val emptyElements = Chain(segment.reverse.takeWhile(_ == '/').map(_ => "").toList: _*)

    val stripSuffix = segments.initLast match {
      case Some((init, "")) => init
      case _ => segments
    }
    val newChain = stripSuffix ++ nonEmpty ++ emptyElements

    this match {
      case AbsolutePath(_, _) =>
        AbsolutePath("/" + ev(newChain.mkString_("/")), newChain)
      case RootlessPath(_, _) =>
        RootlessPath(newChain.mkString_("/"), newChain)
    }
  }

  def `//`(segment: Option[String])(implicit ev: String =:= A): Path[String] = segment match {
    case Some(s) => `//`(s)
    case None => this.map(ev.flip)
  }

  /**
   * Ensure that path always is suffixed with '/'
   */
  def `//`(segment: String)(implicit ev: String =:= A): Path[String] =
    this / (if (segment.endsWith("/")) segment else segment + "/")

  def as[B](b: B): Path[B] = this match {
    case AbsolutePath(_, segments) => AbsolutePath(b, segments)
    case RootlessPath(_, segments) => RootlessPath(b, segments)
  }

  /**
    * Adds a segment to the path while ensuring that the segments and path representation are kept in sync
    *
    * If you're just working with String paths, see [[/]]
    */
  def addSegment[B](segment: String, representation: B): Path[B] =
    (plain / segment).as(representation)

  override def toString: String = Show[Path[A]].show(this)
  override def equals(obj: Any): Boolean =
    obj match {
      case p: Path[_] => p.plain.eqv(plain)
      case _ => false
    }
  override def hashCode(): Int = representation.hashCode()

  def isEmpty: Boolean = segments.isEmpty

  def lastSegment: Option[String] = if(isEmpty) None else {
    val slashSuffix = segments.reverse.takeWhile(_.isEmpty).map(_ => "/").toList.mkString
    val lastNonEmpty = segments.reverse.dropWhile(_.isEmpty).headOption

    lastNonEmpty.map(_ + slashSuffix)
  }

  def fullName(implicit B: FileSystemObject[A]): String              = FileSystemObject[A].name(representation)
  def size(implicit B: FileSystemObject[A]): Option[Long]            = FileSystemObject[A].size(representation)
  def isDir(implicit B: FileSystemObject[A]): Boolean                = FileSystemObject[A].isDir(representation)
  def lastModified(implicit B: FileSystemObject[A]): Option[Instant] = FileSystemObject[A].lastModified(representation)
  def fileName(implicit B: FileSystemObject[A]): Option[String] = if (isDir) None else lastSegment
  def dirName(implicit B: FileSystemObject[A]): Option[String] = if (!isDir) None else lastSegment

  def javaPath: java.nio.file.Path = segments.toList match {
    case h :: t => Paths.get(h, t: _*)
    case Nil => Paths.get(Show[Path.Plain].show(plain))
  }

}

object Path {

  /**
    * A plain path represented with a String
    *
    * See .apply for creating these
    */
  type Plain = Path[String]

  /**
    * A file system object represented with a "general" structure
    *
    * This is typically used in stores that abstracts multiple file systems
    */
  type GeneralObject = Path[UniversalFileSystemObject]

  case class AbsolutePath[A] private (representation: A, segments: Chain[String]) extends Path[A]

  object AbsolutePath {
    def createFrom(s: String): AbsolutePath[String] = {
      if (s.isEmpty || s === "/") AbsolutePath("/", Chain.empty)
      else {
        val nonEmpty = s.stripPrefix("/").split("/").toList
        val empty = s.reverse.takeWhile(_ == '/').map(_ => "").toList
        val chain = Chain(nonEmpty ++ empty : _*)

        AbsolutePath("/" + chain.mkString_("/"), chain)
      }
    }
    def parse(s: String): Option[AbsolutePath[String]] = {
      if (s === "/") AbsolutePath("/", Chain.empty).some
      else if (s.startsWith("/")) {
        val nonEmpty = s.stripPrefix("/").split("/").toList
        val empty = s.reverse.takeWhile(_ == '/').map(_ => "").toList
        Some(AbsolutePath(s, Chain(nonEmpty ++ empty: _*)))
      }
      else None
    }

    val root: AbsolutePath[String] = AbsolutePath("/", Chain.empty)

    implicit def show[A]: Show[AbsolutePath[A]] = "/" + _.segments.mkString_("/")
    implicit def order[A: Order]: Order[AbsolutePath[A]] = (x, y) =>
      Order[A].compare(x.representation, y.representation)
  }

  case class RootlessPath[A] private (representation: A, segments: Chain[String]) extends Path[A]

  object RootlessPath {
    def parse(s: String): Option[RootlessPath[String]] =
      if (!s.startsWith("/")) {
        val nonEmpty = s.split("/").toList
        val empty = s.reverse.takeWhile(_ == '/').map(_ => "").toList
        Some(RootlessPath(s, Chain(nonEmpty ++ empty: _*)))
      }
      else None

    def createFrom(s: String): RootlessPath[String] = {
      if (s.startsWith("/")) {
        createFrom(s.stripPrefix("/"))
      } else {
        val nonEmpty = s.split("/").toList
        val empty = s.reverse.takeWhile(_ == '/').map(_ => "").toList
        RootlessPath(s, Chain(nonEmpty ++ empty: _*))
      }
    }

    def relativeHome: RootlessPath[String] = RootlessPath("", Chain.empty)

    implicit def show[A]: Show[RootlessPath[A]] = _.segments.mkString_("/")
    implicit def order[A: Order]: Order[RootlessPath[A]] = (x, y) =>
      Order[A].compare(x.representation, y.representation)
    implicit def ordering[A: Ordering]: Ordering[RootlessPath[A]] = order[A](Order.fromOrdering[A]).toOrdering
  }

  def empty: RootlessPath[String] = RootlessPath("", Chain.empty)

  def createAbsolute(path: String): AbsolutePath[String] = {
    val noPrefix = path.stripPrefix("/")
    AbsolutePath("/" + noPrefix, Chain(noPrefix.split("/").toList: _*))
  }

  def createRootless(path: String): RootlessPath[String] = {
    val noPrefix = path.stripPrefix("/")
    RootlessPath(noPrefix, Chain(noPrefix.split("/").toList: _*))
  }

  def absolute(path: String): Option[AbsolutePath[String]] = AbsolutePath.parse(path)

  def rootless(path: String): Option[RootlessPath[String]] = RootlessPath.parse(path)

  def plain(path: String): Path.Plain = apply(path)

  def apply(s: String): Path.Plain =
    AbsolutePath.parse(s).orElse(RootlessPath.parse(s)).getOrElse(
      RootlessPath(
        s,
        Chain(s.split("/").toList: _*)
      ) // Paths either have a root or they don't, this block is never executed
    )

  def of[B](path: String, b: B): Path[B] = Path(path).as(b)

  def compare[A](one: Path[A], two: Path[A]): Int = {
    one.show compare two.show
  }

  implicit def order[A: Order]: Order[Path[A]]          = compare
  implicit def ordering[A: Ordering]: Ordering[Path[A]] = order[A](Order.fromOrdering[A]).toOrdering
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
