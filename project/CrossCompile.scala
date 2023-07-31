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
          compilerPlugin("org.typelevel" %% "kind-projector"     % "0.13.2" cross CrossVersion.full),
          compilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1")
        )
      case _ => Nil
    }),
    tpolecatScalacOptions ~= {
      _ ++ List(
        ScalacOptions.sourceFuture,
        ScalacOptions.source3,
        ScalacOptions.languageFeatureOption(
          "adhocExtensions",
          version => ScalaVersion.scalaVersionOrdering.gteq(version, ScalaVersion.V3_0_0)
        )
      )
    }
  )

}
