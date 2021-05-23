name := "azure"

val fs2Version = "3.0.4"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.11.1",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.39.4" % Test
)
