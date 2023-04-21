name := "sftp"

libraryDependencies ++= Seq(
  "com.github.mwiede" % "jsch"                      % "0.2.7",
  "com.dimafeng"     %% "testcontainers-scala-core" % "0.40.15" % Test
)
