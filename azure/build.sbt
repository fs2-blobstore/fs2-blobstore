name := "azure"

val fs2Version = "3.9.4"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.25.1",
  "com.azure"     % "azure-storage-blob-batch"  % "12.21.2",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.41.2" % Test
)
