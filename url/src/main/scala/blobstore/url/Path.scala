package blobstore.url

import blobstore.url.Path.{AbsolutePath, RootlessPath}
import blobstore.url.general.StorageClassLookup
import cats.Show
import cats.data.Chain
import cats.kernel.Order
import cats.syntax.all._

import java.nio.file.Paths
import java.time.Instant
import scala.annotation.tailrec

/** The path segment of a URI. It is parameterized on the type representing the path. This can be a plain String, or a
  * storage provider specific type.
  *
  * Examples of storage provider types would be `software.amazon.awssdk.services.s3.internal.resource.S3ObjectResource`
  * for S3, com.google.storage.Blob for GCS, etc.
  *
  * @see https://www.ietf.org/rfc/rfc3986.txt chapter 3.3, Path
  */
sealed trait Path[+A] {
  def representation: A
  def segments: Chain[String]
  def value: String = Show[Path[A]].show(this)

  def plain: Path.Plain = this match {
    case AbsolutePath(_, segments) => AbsolutePath("/" + segments.mkString_("/"), segments)
    case RootlessPath(_, segments) => RootlessPath(segments.mkString_("/"), segments)
  }

  def absolute: AbsolutePath[String] = plain match {
    case p @ AbsolutePath(_, _)    => p
    case RootlessPath(a, segments) => AbsolutePath("/" + a, segments)
  }

  def relative: RootlessPath[String] = plain match {
    case AbsolutePath(a, segments) => RootlessPath(a.stripPrefix("/"), segments)
    case p @ RootlessPath(_, _)    => p
  }

  def nioPath: java.nio.file.Path = Paths.get(toString)

  /** Compose with string to form a new Path
    *
    * The underlying representation must be String in order for the representation and the path to be kept in sync.
    * Use [[addSegment]] to modify paths backed by non-String types
    *
    * @see addSegment
    */
  def /(segment: String): Path[String] = {
    val nonEmpty      = Chain(segment.stripPrefix("/").split("/").toList: _*)
    val emptyElements = Chain(segment.reverse.takeWhile(_ == '/').map(_ => "").toList: _*)

    val stripSuffix = segments.initLast match {
      case Some((init, "")) => init
      case _                => segments
    }
    val newChain = stripSuffix ++ nonEmpty ++ emptyElements

    this match {
      case AbsolutePath(_, _) =>
        AbsolutePath("/" + newChain.mkString_("/"), newChain)
      case RootlessPath(_, _) =>
        RootlessPath(newChain.mkString_("/"), newChain)
    }
  }

  def /(segment: Option[String]): Path[String] = segment match {
    case Some(s) => /(s)
    case None    => this.plain
  }

  def `//`(segment: Option[String]): Path[String] = segment match {
    case Some(s) => `//`(s)
    case None    => this.plain
  }

  /** Ensure that path always is suffixed with '/'
    */
  def `//`(segment: String): Path[String] =
    /(if (segment.endsWith("/")) segment else segment + "/")

  def as[B](b: B): Path[B] = this match {
    case AbsolutePath(_, segments) => AbsolutePath(b, segments)
    case RootlessPath(_, segments) => RootlessPath(b, segments)
  }

  /** Adds a segment to the path while ensuring that the segments and path representation are kept in sync
    *
    * If you're just working with String paths, see `/`
    */
  def addSegment[B](segment: String, representation: B): Path[B] =
    (plain / segment).as(representation)

  override def toString: String = Show[Path[A]].show(this)

  @SuppressWarnings(Array("scalafix:Disable.Any"))
  override def equals(obj: Any): Boolean =
    obj match {
      case p: Path[_] => p.plain.eqv(plain)
      case _          => false
    }
  override def hashCode(): Int = representation.hashCode()

  def isEmpty: Boolean = segments.isEmpty

  def lastSegment: Option[String] =
    if (isEmpty) None
    else {
      val slashSuffix  = segments.reverse.takeWhile(_.isEmpty).map(_ => "/").toList.mkString
      val lastNonEmpty = segments.reverse.dropWhile(_.isEmpty).headOption

      lastNonEmpty.map(_ + slashSuffix)
    }

  /** Goes one level "up" and looses any information about the underlying path representation
    */
  def up: Path.Plain = {
    @tailrec
    def dropLastSegment(segments: Chain[String], levels: Int): Chain[String] = segments.initLast match {
      case Some((init, last)) => if (last.isEmpty && levels === 0) dropLastSegment(init, levels + 1) else init
      case None               => segments
    }
    val upSegments     = dropLastSegment(segments, 0)
    val representation = upSegments.mkString_("/")
    plain match {
      case AbsolutePath(_, _) => AbsolutePath(representation, upSegments)
      case RootlessPath(_, _) => RootlessPath(representation, upSegments)
    }
  }

  def parentPath: Path.Plain = up

  def stripSlashSuffix: Path[A] = {
    val newSegments = segments.initLast match {
      case Some((init, "")) => init
      case _                => segments
    }

    this match {
      case AbsolutePath(representation, _) => AbsolutePath(representation, newSegments)
      case RootlessPath(representation, _) => RootlessPath(representation, newSegments)
    }
  }

  def fullName(implicit ev: A <:< FsObject): String              = ev(representation).name
  def size(implicit ev: A <:< FsObject): Option[Long]            = ev(representation).size
  def isDir(implicit ev: A <:< FsObject): Boolean                = ev(representation).isDir
  def lastModified(implicit ev: A <:< FsObject): Option[Instant] = ev(representation).lastModified
  def storageClass[SC](implicit storageClassLookup: StorageClassLookup.Aux[A, SC]): Option[SC] =
    storageClassLookup.storageClass(representation)
  def fileName(implicit ev: A <:< FsObject): Option[String] = if (isDir) None else lastSegment
  def dirName(implicit ev: A <:< FsObject): Option[String]  = if (!isDir) None else lastSegment

}

object Path {

  /** A plain path represented with a String
    *
    * See .apply for creating these
    */
  type Plain         = Path[String]
  type AbsolutePlain = AbsolutePath[String]
  type RootlessPlain = RootlessPath[String]

  case class AbsolutePath[A] private (representation: A, segments: Chain[String]) extends Path[A]

  object AbsolutePath {
    def createFrom(s: String): AbsolutePath[String] = {
      if (s.isEmpty || s === "/") AbsolutePath("/", Chain.empty)
      else {
        val nonEmpty = s.stripPrefix("/").split("/").toList
        val empty    = s.reverse.takeWhile(_ == '/').map(_ => "").toList
        val chain    = Chain(nonEmpty ++ empty: _*)

        AbsolutePath("/" + chain.mkString_("/"), chain)
      }
    }
    def parse(s: String): Option[AbsolutePath[String]] = {
      if (s === "/") AbsolutePath("/", Chain.empty).some
      else if (s.startsWith("/")) {
        val nonEmpty = s.stripPrefix("/").split("/").toList
        val empty    = s.reverse.takeWhile(_ == '/').map(_ => "").toList
        Some(AbsolutePath(s, Chain(nonEmpty ++ empty: _*)))
      } else None
    }

    val root: AbsolutePath[String] = AbsolutePath("/", Chain.empty)

    implicit def show[A]: Show[AbsolutePath[A]] = "/" + _.segments.mkString_("/")
    implicit def order[A: Order]: Order[AbsolutePath[A]] =
      (x, y) => Order[A].compare(x.representation, y.representation)
  }

  case class RootlessPath[A] private (representation: A, segments: Chain[String]) extends Path[A]

  object RootlessPath {
    def parse(s: String): Option[RootlessPath[String]] =
      if (!s.startsWith("/")) {
        val nonEmpty = s.split("/").toList
        val empty    = s.reverse.takeWhile(_ == '/').map(_ => "").toList
        Some(RootlessPath(s, Chain(nonEmpty ++ empty: _*)))
      } else None

    @tailrec
    def createFrom(s: String): RootlessPath[String] = {
      if (s.startsWith("/")) {
        createFrom(s.stripPrefix("/"))
      } else {
        val nonEmpty = s.split("/").toList
        val empty    = s.reverse.takeWhile(_ == '/').map(_ => "").toList
        RootlessPath(s, Chain(nonEmpty ++ empty: _*))
      }
    }

    def root: RootlessPath[String] = RootlessPath("", Chain.empty)

    implicit def show[A]: Show[RootlessPath[A]] = _.segments.mkString_("/")
    implicit def order[A: Order]: Order[RootlessPath[A]] =
      (x, y) => Order[A].compare(x.representation, y.representation)
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

  def apply(s: String): Path.Plain = {
    AbsolutePath.parse(s).widen[Path.Plain].orElse(RootlessPath.parse(s)).getOrElse(
      RootlessPath(
        s,
        Chain(s.split("/").toList: _*)
      ) // Paths either have a root or they don't, this block is never executed
    )
  }

  def of[B](path: String, b: B): Path[B] = Path(path).as(b)

  def cmp[A](one: Path[A], two: Path[A]): Int = {
    one.show compare two.show
  }

  def unapply[A](p: Path[A]): Option[(String, A, Chain[String])] = p match {
    case AbsolutePath(representation, segments) => (p.show, representation, segments).some
    case RootlessPath(representation, segments) => (p.show, representation, segments).some
  }

  implicit def order[A]: Order[Path[A]]       = (x: Path[A], y: Path[A]) => cmp(x, y)
  implicit def ordering[A]: Ordering[Path[A]] = order[A].toOrdering
  implicit def show[A]: Show[Path[A]] = {
    case a: AbsolutePath[A] => a.show
    case r: RootlessPath[A] => r.show
  }

}
