import sbt._
import sbt.Keys._

object Tests extends AutoPlugin {
  override def trigger = allRequirements

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    libraryDependencies ++= Seq(
      "org.scalatest"     %% "scalatest"       % "3.2.9"   % Test,
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % Test
    )
  )

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    Test / fork := true
  )
}
