name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.1.72",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.40.0" % Test
)
