name := "azure"

val fs2Version = "3.7.0"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.23.0",
  "com.azure"     % "azure-storage-blob-batch"  % "12.18.3",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.40.17" % Test
)
