import sbt._
import sbt.Keys._

object Tests extends AutoPlugin {
  override def trigger = allRequirements

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    libraryDependencies ++= Seq(
      "com.disneystreaming" %% "weaver-cats"       % "0.7.3" % Test,
      "com.disneystreaming" %% "weaver-scalacheck" % "0.7.3" % Test
    ),
    testFrameworks += new TestFramework("weaver.framework.CatsEffect")
  )

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    Test / fork := true
  )
}
