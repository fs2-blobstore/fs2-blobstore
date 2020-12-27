name := "azure"

val fs2Version = "2.5.0"

libraryDependencies ++= Seq(
  "com.azure"     % "azure-storage-blob"        % "12.9.0",
  "co.fs2"       %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng" %% "testcontainers-scala-core" % "0.38.7" % Test
)
