name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "2.28.0",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.44.1" % Test
)
