name := "core"

val fs2Version = "2.3.0"

libraryDependencies ++= Seq(
  "co.fs2"        %% "fs2-core"         % fs2Version,
  "co.fs2"        %% "fs2-io"           % fs2Version,
  "com.chuusai"   %% "shapeless"        % "2.3.3",
  "org.scalatest" %% "scalatest"        % "3.2.0-M4" % Test,
  "org.typelevel" %% "cats-effect-laws" % "2.1.3"    % Test
) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
  case Some((2, x)) if x < 13 =>
    "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6" :: Nil
  case _ =>
    Nil
})
