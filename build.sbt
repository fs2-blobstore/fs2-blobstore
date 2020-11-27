inThisBuild(
  Seq(
    scalaVersion := "2.13.4",
    crossScalaVersions := Seq("2.12.12", "2.13.4"),
    organization := "com.github.fs2-blobstore",
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer("rolandomanrique", "Rolando Manrique", "", url("http://github.com/rolandomanrique")),
      Developer("stew", "Stew O'Connor", "", url("https://github.com/stew")),
      Developer("gafiatulin", "Victor Gafiatulin", "", url("https://github.com/gafiatulin")),
      Developer("jgogstad", "Jostein Gogstad", "", url("https://github.com/jgogstad"))
    ),
    homepage := Some(url("https://github.com/fs2-blobstore/fs2-blobstore")),
    startYear := Some(2018),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
  )
)

lazy val fs2blobstore = project
  .in(file("."))
  .settings(
    moduleName := "root",
    skip in publish := true
  )
  .aggregate(core, s3, sftp, box, gcs, azure)

lazy val core = project

lazy val s3 = project.dependsOn(core % "compile->compile;test->test")

lazy val sftp = project.dependsOn(core % "compile->compile;test->test")

lazy val box = project.dependsOn(core % "compile->compile;test->test")

lazy val gcs = project.dependsOn(core % "compile->compile;test->test")

lazy val azure = project.dependsOn(core % "compile->compile;test->test")

lazy val docs = (project in file("project-docs"))
  .settings(
    mdocVariables := Map(
      "VERSION" -> version.value
    ),
    skip in publish := true,
    Compile / scalacOptions -= "-Ywarn-dead-code",
    mdocExtraArguments := Seq("--no-link-hygiene") // https://github.com/scalameta/mdoc/issues/94
  )
  .dependsOn(azure, gcs, box, sftp, s3, azure, core % "compile->test")
  .enablePlugins(MdocPlugin)
