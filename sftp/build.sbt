name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.22",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.41.5" % Test
)
