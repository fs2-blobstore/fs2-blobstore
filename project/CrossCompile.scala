import io.github.davidgregory084.TpolecatPlugin
import sbt.Keys._
import sbt.{Def, _}

object CrossCompile extends AutoPlugin {
  override def trigger = allRequirements

  override def requires = TpolecatPlugin && Scalafix

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => List(
          compilerPlugin("org.typelevel" %% "kind-projector"     % "0.13.1" cross CrossVersion.full),
          compilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1")
        )
      case _ => Nil
    }),
    scalacOptions := (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((3, _)) =>
        "-source:future" :: scalacOptions.value.map {
          case "-Ykind-projector" => "-Ykind-projector:underscores"
          case x                  => x
        }.toList
      case Some((2, 13)) | Some((2, 12)) =>
        scalacOptions.value ++ List("-Xsource:3", "-P:kind-projector:underscore-placeholders")
      case _ => scalacOptions.value
    }),
    // TODO: Remove this after switching from scalatest to weaver-test
    Test / scalacOptions ~= {
      _.map {
        case x if x.startsWith("-source") => "-source:3.0"
        case x                            => x
      }
    }
  )

}
