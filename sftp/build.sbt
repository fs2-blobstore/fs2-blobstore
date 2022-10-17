name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.4",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.40.10" % Test
)
