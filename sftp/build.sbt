name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.11",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.41.0" % Test
)
