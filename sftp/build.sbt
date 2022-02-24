name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.1.72",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.40.2" % Test
)
