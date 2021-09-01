name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.1.65",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.39.5" % Test
)
