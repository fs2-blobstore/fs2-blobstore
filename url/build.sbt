name := "url"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "2.4.2",
  "com.chuusai"   %% "shapeless" % "2.3.3"
) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
  case Some((2, x)) if x < 13 =>
    "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.1" :: Nil
  case _ =>
    Nil
})
