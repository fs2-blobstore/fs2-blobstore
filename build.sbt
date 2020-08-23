inThisBuild(
  Seq(
    scalaVersion := "2.13.3",
    crossScalaVersions := Seq("2.12.12", "2.13.3"),
    organization := "com.github.fs2-blobstore",
    licenses := List("Apache-2.0" -> sbt.url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer("rolandomanrique", "Rolando Manrique", "", sbt.url("http://github.com/rolandomanrique")),
      Developer("stew", "Stew O'Connor", "", sbt.url("https://github.com/stew")),
      Developer("gafiatulin", "Victor Gafiatulin", "", sbt.url("https://github.com/gafiatulin")),
      Developer("jgogstad", "Jostein Gogstad", "", sbt.url("https://github.com/jgogstad"))
    ),
    homepage := Some(sbt.url("https://github.com/fs2-blobstore/fs2-blobstore")),
    startYear := Some(2018),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full),
    libraryDependencies ++= Seq(
      compilerPlugin("com.github.ghik" % "silencer-plugin" % "1.7.0" cross CrossVersion.full),
      "com.github.ghik" % "silencer-lib" % "1.7.0" % Provided cross CrossVersion.full
    )
  )
)

lazy val fs2blobstore = project
  .in(file("."))
  .settings(
    moduleName := "root",
    skip in publish := true
  )
  .aggregate(url, core, s3, sftp, box, gcs, azure)

lazy val url = project

lazy val core = project.dependsOn(url)

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
