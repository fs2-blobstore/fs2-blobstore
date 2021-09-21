name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.1.66",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.39.8" % Test
)
