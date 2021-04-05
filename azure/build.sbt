name := "azure"

val fs2Version = "2.5.4"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.10.1",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.39.3" % Test
)
