name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.1",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.40.9" % Test
)
