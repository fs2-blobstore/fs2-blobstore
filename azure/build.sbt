name := "azure"

val fs2Version = "3.2.5"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.15.0",
  "com.azure"     % "azure-storage-blob-batch"  % "12.12.0",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.40.7" % Test
)
