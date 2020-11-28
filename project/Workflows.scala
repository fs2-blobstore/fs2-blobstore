import sbt._
import sbtghactions.GenerativePlugin.autoImport._
import CoursierWorkflow.autoImport._
import ReleaseWorkflow.autoImport._

object Workflows extends AutoPlugin {
  override def requires: Plugins = ReleaseWorkflow && CoursierWorkflow

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    githubWorkflowGeneratedCI := {
      def testSteps(tag: String, sbtTestSteps: WorkflowStep*): List[WorkflowStep] =
        WorkflowStep.Checkout ::
          coursierSetup.value ::
          coursierCache.value ::
          checkWorkflows.value ::
          Sbtn(
            name = Some("Checks"),
            commands = List(
              "scalafmtSbtCheck",
              "scalafmtCheckAll",
              "compile",
              "doc",
              "docs/mdoc",
              "scalafix --check"
            ).mkString("; ") :: Nil
          ) ::
          WorkflowStep.Run(name = Some("Create tmp directory"), commands = List("mkdir tmp")) ::
          sbtTestSteps.toList :::
          WorkflowStep.Use(
            name = Some("Upload code coverage to codecov"),
            owner = "codecov",
            repo = "codecov-action",
            ref = "v1",
            params = Map("flags" -> s"$tag-scala-$${{ matrix.scala }}")
          ) ::
          Nil

      val noItTest = WorkflowJob(
        id = "unit-test",
        name = "Static checks and tests",
        cond =
          Some(
            "github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name != github.repository"
          ),
        oses = githubWorkflowOSes.value.toList,
        scalas = Keys.crossScalaVersions.value.toList,
        javas = githubWorkflowJavaVersions.value.toList,
        steps = testSteps(
          "unit",
          Sbtn(
            name = Some("Test"),
            commands =
              List("coverage", "testOnly * -- -l blobstore.IntegrationTest", "coverageAggregate").mkString("; ") :: Nil
          )
        )
      )

      val allTest = WorkflowJob(
        id = "all-test",
        name = "Static checks and tests",
        cond =
          Some(
            "github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository"
          ),
        oses = githubWorkflowOSes.value.toList,
        scalas = Keys.crossScalaVersions.value.toList,
        javas = githubWorkflowJavaVersions.value.toList,
        steps = testSteps(
          "all",
          WorkflowStep.Run(
            name = Some("Decrypt Box App Key"),
            commands = List(
              "openssl aes-256-cbc -K ${{ secrets.OPENSSL_KEY }} -iv ${{ secrets.OPENSSL_IV }} -in box/src/test/resources/box_appkey.json.enc -out box/src/test/resources/box_appkey.json -d"
            )
          ),
          Sbtn(
            name = Some("Test"),
            commands = List("coverage", "test", "coverageAggregate").mkString("; ") :: Nil
          )
        )
      )

      val publish = WorkflowJob(
        id = "publish",
        name = "Publish artifacts",
        cond =
          Some("github.event_name == 'push' && github.ref == 'refs/heads/master'"),
        oses = githubWorkflowOSes.value.toList,
        scalas = Keys.crossScalaVersions.value.toList,
        javas = githubWorkflowJavaVersions.value.toList,
        steps =
          WorkflowStep.Checkout ::
            coursierSetup.value ::
            coursierCache.value ::
            checkWorkflows.value ::
            WorkflowStep.Use(
              "olafurpg",
              "setup-gpg",
              "v3"
            ) ::
            Sbtn(
              name = Some("CI Release"),
              commands = List("ci-release"),
              env = Map(
                "PGP_PASSPHRASE"    -> "${{ secrets.PGP_PASSPHRASE }}",
                "PGP_SECRET"        -> "${{ secrets.PGP_SECRET }}",
                "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}",
                "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}"
              )
            ) ::
            Nil
      )

      Seq(noItTest, allTest, publish)
    }
  )
}
