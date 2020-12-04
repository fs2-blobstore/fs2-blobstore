package blobstore.url.exception

import cats.Semigroup

import scala.annotation.tailrec
import scala.util.Try
import cats.syntax.show._

object Throwables {

  /** @return A new Throwable where `y` is appended as a root cause to `x`. We prefix `y`'s message with
    *         "APPENDED: " to make the distinction clear in x's stack trace
    */
  val collapsingSemigroup: Semigroup[Throwable] = (x, y) => {
    /*
    Give it our best in spite of
    https://bugs.openjdk.java.net/browse/JDK-8057919
    https://bugs.openjdk.java.net/browse/JDK-8187123
     */
    val elWithAppend = Try(y.getClass.getName)
      .map(cn => new Exception(show"APPENDED: $cn: ${y.getMessage}", y.getCause))
      .getOrElse(new Exception(show"APPENDED: ${y.getMessage}", y)) // Introduce an extra layer on failure

    getRoot(x).initCause(elWithAppend)
  }

  @tailrec
  private def getRoot(i: Throwable): Throwable = if (Option(i.getCause).isDefined) getRoot(i.getCause) else i
}
