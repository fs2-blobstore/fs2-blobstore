name := "azure"

val fs2Version = "3.12.2"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.33.0",
  "com.azure"     % "azure-storage-blob-batch"  % "12.28.0",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.44.1" % Test
)
