name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.0",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.40.7" % Test
)
