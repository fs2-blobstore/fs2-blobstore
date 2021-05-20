name := "sftp"

libraryDependencies ++= Seq(
  "com.jcraft"    % "jsch"                      % "0.1.55",
  "com.dimafeng" %% "testcontainers-scala-core" % "0.39.4" % Test
)
