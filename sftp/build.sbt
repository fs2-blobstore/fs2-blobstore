name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "2.27.0",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.43.0" % Test
)
