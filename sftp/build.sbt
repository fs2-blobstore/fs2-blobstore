name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "2.27.3",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.43.6" % Test
)
