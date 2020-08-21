package blobstore.url

import cats.{Order, Show}
import cats.instances.option._
import cats.instances.string._
import cats.instances.tuple._
import cats.syntax.all._

case class UserInfo private (user: String, password: Option[String] = None) extends Ordering[UserInfo] {
  override def compare(x: UserInfo, y: UserInfo): Int = UserInfo.compare(x, y)

  override def toString: String = show"$user${password.as(":*****").getOrElse("").show}"

  def toStringWithPassword = show"$user${password.fold("")(":" + _)}"
}

object UserInfo {

  implicit val order: Order[UserInfo] = Order.by { userInfo: UserInfo =>
    (userInfo.user, userInfo.password.getOrElse(""))
  }

  implicit val show: Show[UserInfo] = Show.fromToString

  def compare(one: UserInfo, two: UserInfo): Int = order.compare(one, two)

}