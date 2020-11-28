import sbt._
import sbtghactions.GenerativePlugin
import sbtghactions.GenerativePlugin.autoImport._

object CoursierWorkflow extends AutoPlugin {
  override def requires: Plugins = GenerativePlugin

  object autoImport {
    val coursierSetup = settingKey[WorkflowStep]("coursier setup step")
    val coursierCache = settingKey[WorkflowStep]("coursier cache step")
  }

  import autoImport._

  override lazy val buildSettings = Seq(
    githubWorkflowSbtCommand := "sbtn",
    githubWorkflowJavaVersions := Seq("adopt:11"),
    coursierSetup := {
      WorkflowStep.Use(
        "laughedelic",
        "coursier-setup",
        "v1",
        Map("jvm" -> s"$${{ matrix.java }}")
      )
    },
    coursierCache := {
      WorkflowStep.Use(
        "coursier",
        "cache-action",
        "v5"
      )
    }
  )
}
