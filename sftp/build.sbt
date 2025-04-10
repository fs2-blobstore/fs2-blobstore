name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.25",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.43.0" % Test
)
