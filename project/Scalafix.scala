import sbt.{Def, _}
import sbt.Keys._
import scalafix.sbt.ScalafixPlugin
import scalafix.sbt.ScalafixPlugin.autoImport._
import org.typelevel.sbt.tpolecat.TpolecatPlugin
import org.typelevel.sbt.tpolecat.TpolecatPlugin.autoImport._
import org.typelevel.scalacoptions.{ScalaVersion, ScalacOption, ScalacOptions}

object Scalafix extends AutoPlugin {

  override def trigger  = allRequirements
  override def requires = ScalafixPlugin && TpolecatPlugin

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    tpolecatScalacOptions ~= {
      _ ++ List(
        ScalacOptions.privateOption(
          "rangepos",
          version => version.isBetween(ScalaVersion.V2_12_0, ScalaVersion.V3_0_0)
        ),
        ScalacOption(
          "-P:semanticdb:synthetics:on",
          version => version.isBetween(ScalaVersion.V2_12_0, ScalaVersion.V3_0_0)
        )
      )
    }
  )

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    semanticdbEnabled                             := true,
    semanticdbVersion                             := scalafixSemanticdb.revision,
    scalafixDependencies += "com.github.vovapolu" %% "scaluzzi" % "0.1.23"
  )
}
