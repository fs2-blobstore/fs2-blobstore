name := "azure"

val fs2Version = "3.12.0"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.30.1",
  "com.azure"     % "azure-storage-blob-batch"  % "12.27.1",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.43.0" % Test
)
