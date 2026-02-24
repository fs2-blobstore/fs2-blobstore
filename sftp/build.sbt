name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "2.27.8",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.44.1" % Test
)
