name := "azure"

val fs2Version = "3.12.2"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.31.2",
  "com.azure"     % "azure-storage-blob-batch"  % "12.27.3",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.43.0" % Test
)
