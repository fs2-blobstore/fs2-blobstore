import sbt.{Def, _}
import sbt.Keys._
import scalafix.sbt.ScalafixPlugin
import scalafix.sbt.ScalafixPlugin.autoImport._

object Scalafix extends AutoPlugin {

  override def trigger  = allRequirements
  override def requires = ScalafixPlugin

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((3, _)) => Seq.empty
      case Some((2, _)) => List(
          "-Yrangepos", // required by SemanticDB compiler plugin
          "-P:semanticdb:synthetics:on"
        )
      case _ => Seq.empty
    })
  )

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    semanticdbEnabled                             := true,
    semanticdbVersion                             := scalafixSemanticdb.revision,
    scalafixDependencies += "com.github.vovapolu" %% "scaluzzi" % "0.1.23"
  )
}
