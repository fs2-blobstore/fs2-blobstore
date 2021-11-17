name := "azure"

val fs2Version = "3.1.6"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.14.1",
  "com.azure"     % "azure-storage-blob-batch"  % "12.11.2",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.39.11" % Test
)
