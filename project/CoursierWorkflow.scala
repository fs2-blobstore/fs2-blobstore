import sbt._
import sbtghactions.GenerativePlugin
import sbtghactions.GenerativePlugin.autoImport._

object CoursierWorkflow extends AutoPlugin {
  override def requires: Plugins = GenerativePlugin

  object autoImport {
    val coursierSetup = settingKey[WorkflowStep]("coursier setup step")
    val coursierCache = settingKey[WorkflowStep]("coursier cache step")

    def Sbtn(
      commands: List[String],
      id: Option[String] = None,
      name: Option[String] = None,
      cond: Option[String] = None,
      env: Map[String, String] = Map()
    ): WorkflowStep = {

      def indent(output: String, level: Int): String = {
        val space = (0 until level * 2).map(_ => ' ').mkString
        (space + output.replace("\n", s"\n$space")).replaceAll("""\n[ ]+\n""", "\n\n")
      }

      def isSafeString(str: String): Boolean =
        !(str.indexOf(':') >= 0 || // pretend colon is illegal everywhere for simplicity
          str.indexOf('#') >= 0 || // same for comment
          str.indexOf('!') == 0 ||
          str.indexOf('*') == 0 ||
          str.indexOf('-') == 0 ||
          str.indexOf('?') == 0 ||
          str.indexOf('{') == 0 ||
          str.indexOf('}') == 0 ||
          str.indexOf('[') == 0 ||
          str.indexOf(']') == 0 ||
          str.indexOf(',') == 0 ||
          str.indexOf('|') == 0 ||
          str.indexOf('>') == 0 ||
          str.indexOf('@') == 0 ||
          str.indexOf('`') == 0 ||
          str.indexOf('"') == 0 ||
          str.indexOf('\'') == 0 ||
          str.indexOf('&') == 0)

      def wrap(str: String): String =
        if (str.indexOf('\n') >= 0)
          "|\n" + indent(str, 1)
        else if (isSafeString(str))
          str
        else
          s"'${str.replace("'", "''")}'"

      val safeCommands = commands map { c =>
        if (c.indexOf(' ') >= 0)
          s"'$c'"
        else
          c
      }

      val sbtnCommand = wrap(s"sbtn ++$${{ matrix.scala }}! ${safeCommands.mkString(" ")}")
      WorkflowStep.Run(sbtnCommand :: Nil, id, name, cond, env)
    }
  }

  import autoImport._

  override lazy val buildSettings = Seq(
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
