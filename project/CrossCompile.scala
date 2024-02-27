import sbt.Keys._
import sbt.{Def, _}
import org.typelevel.sbt.tpolecat.TpolecatPlugin
import org.typelevel.sbt.tpolecat.TpolecatPlugin.autoImport._
import org.typelevel.scalacoptions.{ScalaVersion, ScalacOptions}

object CrossCompile extends AutoPlugin {
  override def trigger = allRequirements

  override def requires = TpolecatPlugin && Scalafix

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => List(
          compilerPlugin("org.typelevel" %% "kind-projector"     % "0.13.3" cross CrossVersion.full),
          compilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1")
        )
      case _ => Nil
    }),
    tpolecatScalacOptions ++= Set(
      ScalacOptions.sourceFuture,
      ScalacOptions.source3,
      ScalacOptions.languageFeatureOption("adhocExtensions", _.isAtLeast(ScalaVersion.V3_0_0)),
      // we want to opt-in to the -Xsource:3 semantics changes, and opt-out from fatal warnings about the changes
      ScalacOptions.warnOption("conf:cat=scala3-migration:s", _.isBetween(ScalaVersion.V2_13_9, ScalaVersion.V3_0_0))
    )
  )

}
