name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.16",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.41.2" % Test
)
