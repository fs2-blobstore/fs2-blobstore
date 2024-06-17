name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.18",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.41.4" % Test
)
