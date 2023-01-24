name := "azure"

val fs2Version = "3.3.0"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.20.0",
  "com.azure"     % "azure-storage-blob-batch"  % "12.16.2",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.40.12" % Test
)
