import sbt._
import sbt.Keys._
import _root_.io.github.davidgregory084.TpolecatPlugin
import _root_.io.github.davidgregory084.TpolecatPlugin.autoImport._

object Tests extends AutoPlugin {
  override def trigger  = allRequirements
  override def requires = TpolecatPlugin

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    libraryDependencies ++= Seq(
      "org.scalatest"     %% "scalatest"       % "3.2.15"   % Test,
      "org.scalatestplus" %% "scalacheck-1-17" % "3.2.15.0" % Test
    )
  )

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    Test / fork := true,
    Test / tpolecatScalacOptions ~= { _ - ScalacOptions.warnNonUnitStatement }
  )
}
