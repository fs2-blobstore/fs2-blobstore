name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.1.67",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.39.7" % Test
)
