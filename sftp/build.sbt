name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "2.27.2",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.43.0" % Test
)
