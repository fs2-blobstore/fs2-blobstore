package blobstore.url

import cats.{Order, Show}
import cats.instances.option._
import cats.instances.string._
import cats.instances.tuple._
import cats.syntax.all._

case class UserInfo(user: String, password: Option[String] = None) {
  override def toString: String = show"$user${password.as(":*****").getOrElse("").show}"

  def toStringWithPassword = show"$user${password.fold("")(":" + _)}"
}

object UserInfo {

  implicit val order: Order[UserInfo] = Order.by { userInfo: UserInfo =>
    (userInfo.user, userInfo.password.getOrElse(""))
  }
  implicit val ordering: Ordering[UserInfo] = order.toOrdering

  implicit val show: Show[UserInfo] = Show.fromToString

}
