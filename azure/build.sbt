name := "azure"

val fs2Version = "3.0.1"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.10.2",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.39.3" % Test
)
