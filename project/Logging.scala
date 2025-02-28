import sbt.{Def, _}
import sbt.Keys._
import sbt.plugins.JvmPlugin
object Logging extends AutoPlugin {

  override def trigger = allRequirements

  override def requires = JvmPlugin

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.17" % Test
  )
}
