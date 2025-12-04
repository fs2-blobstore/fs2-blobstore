name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "2.27.7",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.44.0" % Test
)
