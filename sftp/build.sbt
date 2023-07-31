name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.10",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.40.17" % Test
)
