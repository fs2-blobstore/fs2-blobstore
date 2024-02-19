import sbt._
import sbt.Keys._
import org.typelevel.sbt.tpolecat.TpolecatPlugin
import org.typelevel.sbt.tpolecat.TpolecatPlugin.autoImport._
import org.typelevel.scalacoptions.ScalacOptions

object Tests extends AutoPlugin {
  override def trigger  = allRequirements
  override def requires = TpolecatPlugin

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    libraryDependencies ++= Seq(
      "org.scalatest"     %% "scalatest"       % "3.2.17"   % Test,
      "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % Test
    )
  )

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    Test / fork := true,
    Test / tpolecatScalacOptions ~= { _ - ScalacOptions.warnNonUnitStatement }
  )
}
