name := "azure"

val fs2Version = "2.5.10"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.14.1",
  "com.azure"     % "azure-storage-blob-batch"  % "12.11.1",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.39.9" % Test
)
