name := "azure"

val fs2Version = "3.2.9"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.18.0",
  "com.azure"     % "azure-storage-blob-batch"  % "12.14.0",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.40.8" % Test
)
