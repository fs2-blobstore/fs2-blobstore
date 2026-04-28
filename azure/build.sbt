name := "azure"

val fs2Version = "3.13.0"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.33.3",
  "com.azure"     % "azure-storage-blob-batch"  % "12.29.2",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.44.1" % Test
)
