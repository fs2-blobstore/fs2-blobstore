name := "azure"

val fs2Version = "3.9.2"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.23.1",
  "com.azure"     % "azure-storage-blob-batch"  % "12.19.1",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.41.0" % Test
)
