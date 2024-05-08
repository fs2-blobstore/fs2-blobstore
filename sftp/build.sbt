name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.17",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.41.3" % Test
)
