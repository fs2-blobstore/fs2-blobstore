ThisBuild / scalaVersion       := "2.13.8"
ThisBuild / crossScalaVersions := Seq("2.12.15", "2.13.8", "3.1.0")
ThisBuild / organization       := "com.github.fs2-blobstore"
ThisBuild / licenses           := List("Apache-2.0" -> sbt.url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer("rolandomanrique", "Rolando Manrique", "", sbt.url("http://github.com/rolandomanrique")),
  Developer("stew", "Stew O'Connor", "", sbt.url("https://github.com/stew")),
  Developer("gafiatulin", "Victor Gafiatulin", "", sbt.url("https://github.com/gafiatulin")),
  Developer("jgogstad", "Jostein Gogstad", "", sbt.url("https://github.com/jgogstad"))
)
ThisBuild / homepage  := Some(sbt.url("https://github.com/fs2-blobstore/fs2-blobstore"))
ThisBuild / startYear := Some(2018)
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

lazy val fs2blobstore = project
  .in(file("."))
  .settings(
    moduleName     := "root",
    publish / skip := true
  )
  .aggregate(url, core, s3, sftp, gcs, azure, box, `integration-tests`)

lazy val url = project

lazy val core = project.dependsOn(url)

lazy val s3 = project.dependsOn(core % "compile->compile;test->test")

lazy val sftp = project.dependsOn(core % "compile->compile;test->test")

lazy val box = project.dependsOn(core % "compile->compile;test->test")

lazy val gcs = project.dependsOn(core % "compile->compile;test->test")

lazy val azure = project.dependsOn(core % "compile->compile;test->test")

lazy val `integration-tests` = project.dependsOn(s3, sftp, box, gcs, azure)

lazy val docs = (project in file("project-docs"))
  .settings(
    mdocVariables := Map(
      "VERSION" -> version.value
    ),
    publish / skip := true,
    Compile / scalacOptions -= "-Ywarn-dead-code",
    mdocExtraArguments := Seq("--no-link-hygiene") // https://github.com/scalameta/mdoc/issues/94
  )
  .dependsOn(gcs, sftp, s3, box, azure, core % "compile->test")
  .enablePlugins(MdocPlugin)
