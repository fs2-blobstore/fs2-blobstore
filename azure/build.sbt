name := "azure"

val fs2Version = "3.5.0"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.20.2",
  "com.azure"     % "azure-storage-blob-batch"  % "12.16.3",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.40.12" % Test
)
