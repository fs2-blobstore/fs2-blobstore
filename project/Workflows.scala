import sbt.AutoPlugin
import sbt.{Def, _}
import sbtghactions.GenerativePlugin
import sbtghactions.GenerativePlugin.autoImport._

object Workflows extends AutoPlugin {
  override def requires = GenerativePlugin
  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    githubWorkflowJavaVersions := Seq("adopt@1.11"),
    // format: off
    githubWorkflowGeneratedCI := {
      def testSteps(tag: String, sbtTestSteps: List[WorkflowStep]): List[WorkflowStep] =
        WorkflowStep.Checkout ::
          WorkflowStep.SetupScala ::
          githubWorkflowGeneratedCacheSteps.value.toList :::
          WorkflowStep.Sbt(name = Some("Check that workflows are up to date"), commands = List("githubWorkflowCheck")) ::
          WorkflowStep.Sbt(name = Some("Checks"), commands = List("clean", "scalafmtSbtCheck", "scalafmtCheckAll", "compile", "doc", "docs/mdoc", "scalafix --check").mkString("; ") :: Nil) ::
          WorkflowStep.Run(name = Some("Create tmp directory"), commands = List("mkdir tmp")) ::
          WorkflowStep.Run(name = Some("Start containers"), commands = List("docker-compose up -d")) ::
          sbtTestSteps :::
          WorkflowStep.Use(name = Some("Upload code coverage to codecov"), owner = "codecov", repo = "codecov-action", ref = "v1", params = Map("flags" -> List(tag, "scala-${{ matrix.scala }}").mkString(", "))) ::
          WorkflowStep.Run(name = Some("Cleanup containers and images"), commands = List("docker-compose down -v --remove-orphans")) ::
          Nil
      List(
        // Running checks and tests on PR's to 'dev' branch made from forks (including scala-steward) – no secrets available. 
        WorkflowJob(
          id = "test-external",
          name = "Test external PR",
          cond = Some("github.event_name == 'pull_request' && github.base_ref == 'dev'"),
          oses = githubWorkflowOSes.value.toList,
          scalas = Keys.crossScalaVersions.value.toList,
          javas = githubWorkflowJavaVersions.value.toList,
          steps = testSteps(
            tag = "unittest",
            sbtTestSteps = WorkflowStep.Sbt(
              name = Some("Test"),
              commands = List("coverage", "testOnly * -- -l blobstore.IntegrationTest", "coverageAggregate").mkString("; ") :: Nil
            ) :: Nil
          )
        ),
        // Running checks and all tests on PR's to 'master' branch made form within repository – secrets available. 
        WorkflowJob(
          id = "test",
          name = "Test PR",
          cond = Some("github.event_name == 'pull_request' && github.base_ref == 'master'"),
          oses = githubWorkflowOSes.value.toList,
          scalas = Keys.crossScalaVersions.value.toList,
          javas = githubWorkflowJavaVersions.value.toList,
          steps = testSteps(
            tag = "alltest",
            sbtTestSteps =
              WorkflowStep.Run(
                name = Some("Decrypt Box App Key"),
                commands = List("openssl aes-256-cbc -K ${{ secrets.OPENSSL_KEY }} -iv ${{ secrets.OPENSSL_IV }} -in box/src/test/resources/box_appkey.json.enc -out box/src/test/resources/box_appkey.json -d")
              ) :: 
              WorkflowStep.Sbt(
                name = Some("Test"),
                commands = List("coverage", "test", "coverageAggregate").mkString("; ") :: Nil,
                env = Map(
                  "S3_TEST_ACCESS_KEY" -> "${{ secrets.S3_TEST_ACCESS_KEY }}",
                  "S3_TEST_SECRET_KEY" -> "${{ secrets.S3_TEST_SECRET_KEY }}",
                  "S3_TEST_BUCKET" -> "${{ secrets.S3_TEST_BUCKET }}"
                )
              ) :: Nil
          )
        ),
        // Promote PR's merged to 'dev' by opening PR to master – secrets available.
        // Want to trigger further workflow runs – can't use ${{ secrets.GITHUB_TOKEN }} to open PR. 
        // Use App to get low-access token that will trigger further runs.
        WorkflowJob(
          id = "promote-external",
          name = "Promote external PR",
          cond = Some("github.event_name == 'push' && github.ref == 'refs/heads/dev'"),
          oses = githubWorkflowOSes.value.headOption.toList,
          steps =
            WorkflowStep.Checkout ::
            WorkflowStep.Use(name = Some("Generate low-access token"), id = Some("generate-token"), owner = "tibdex", repo = "github-app-token", ref = "v1", params = Map("app_id" -> "${{ secrets.APP_ID }}", "private_key" -> "${{ secrets.APP_PRIVATE_KEY }}")) ::
            WorkflowStep.Use(name = Some("Create promoting PR"), owner = "repo-sync", repo = "pull-request", ref = "v2", params = Map("destination_branch" -> "master", "pr_title" -> "Promote External PR", "pr_body" -> "Automatically promote ${{ github.sha }}", "pr_label" -> "auto_pr", "pr_allow_empty" -> "false", "github_token" -> "${{ steps.generate-token.outputs.token }}")) ::
            Nil
        ),
        // Publish on pushes to master
        WorkflowJob(
          id = "publish",
          name = "Publish artifacts",
          cond = Some("github.event_name == 'push' && github.ref == 'refs/heads/master'"),
          oses = githubWorkflowOSes.value.toList,
          scalas = Keys.crossScalaVersions.value.toList,
          javas = githubWorkflowJavaVersions.value.toList,
          steps =
            WorkflowStep.Checkout ::
            WorkflowStep.SetupScala ::
            githubWorkflowGeneratedCacheSteps.value.toList :::
            WorkflowStep.Sbt(name = Some("Check that workflows are up to date"), commands = List("githubWorkflowCheck")) ::
            WorkflowStep.Sbt(name = Some("sbt ci-release"), commands = List("ci-release"), env = Map("PGP_PASSPHRASE" -> "${{ secrets.PGP_PASSPHRASE }}", "PGP_SECRET" -> "${{ secrets.PGP_SECRET }}", "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}", "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}")) ::
            Nil
        )
      )
    }
    // format: on
  )
}
