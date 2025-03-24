name := "azure"

val fs2Version = "3.11.0"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.30.0",
  "com.azure"     % "azure-storage-blob-batch"  % "12.26.0",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.43.0" % Test
)
