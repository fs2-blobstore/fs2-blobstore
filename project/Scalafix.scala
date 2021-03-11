import sbt.{Def, _}
import sbt.Keys._
import scalafix.sbt.ScalafixPlugin
import scalafix.sbt.ScalafixPlugin.autoImport._

object Scalafix extends AutoPlugin {

  override def trigger  = allRequirements
  override def requires = ScalafixPlugin

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    semanticdbEnabled := true,
    scalacOptions ++= List(
      "-Yrangepos", // required by SemanticDB compiler plugin
      "-P:semanticdb:synthetics:on"
    ),
    scalafixDependencies += "com.github.vovapolu" %% "scaluzzi" % "0.1.18"
  )
}
