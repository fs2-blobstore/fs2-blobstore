name := "core"

val fs2Version = "2.5.0"

libraryDependencies ++= Seq(
  "co.fs2"        %% "fs2-core"         % fs2Version,
  "co.fs2"        %% "fs2-io"           % fs2Version,
  "org.typelevel" %% "cats-effect-laws" % "2.2.0" % Test
) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
  case Some((2, x)) if x < 13 =>
    "org.scala-lang.modules" %% "scala-collection-compat" % "2.3.2" :: Nil
  case _ =>
    Nil
})
