inThisBuild(
  Seq(
    scalaVersion := "2.13.6",
    crossScalaVersions := Seq("2.12.13", "2.13.6"),
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
    addCompilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1"),
    addCompilerPlugin("org.typelevel" %% "kind-projector"     % "0.13.0" cross CrossVersion.full),
    javacOptions ++= {
      val javaVersion: Int = {
        var sysVersion = System.getProperty("java.version")
        if (sysVersion.startsWith("1."))
          sysVersion = sysVersion.drop(2)
        sysVersion.split('.').head.toInt
      }
      javaVersion match {
        case x if x < 10  => Seq("-source", s"1.$x", "-target", s"1.$x")
        case x if x >= 10 => Seq("-source", s"$x", "-target", s"$x")
      }
    }
  )
)

lazy val root = project
  .in(file("."))
  .settings(
    publish / skip := true
  )
  .aggregate(url, core, box /*, s3, sftp, gcs, azure, box*/ )

lazy val url = project

lazy val core = project.dependsOn(url)

lazy val s3 = project.dependsOn(core % "compile->compile;test->test")

lazy val sftp = project.dependsOn(core % "compile->compile;test->test")

lazy val box = project
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(core % "compile->compile;it->test")

lazy val gcs = project.dependsOn(core % "compile->compile;test->test")

lazy val azure = project.dependsOn(core % "compile->compile;test->test")

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
