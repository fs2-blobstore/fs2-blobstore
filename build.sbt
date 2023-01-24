ThisBuild / scalaVersion       := "2.13.10"
ThisBuild / crossScalaVersions := Seq("2.12.17", "2.13.10", "3.2.1")
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

lazy val microsite = project
  .settings(
    Compile / scalacOptions --= Seq("-Ywarn-unused:locals", "-Wunused:locals"),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "squants" % "1.8.3"
    )
  )
  .dependsOn(gcs, sftp, s3, box, azure, core % "compile->test")
  .enablePlugins(MdocPlugin, MicrositesPlugin)
