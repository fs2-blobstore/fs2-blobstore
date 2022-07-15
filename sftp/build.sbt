name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.2",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.40.9" % Test
)
