name := "azure"

val fs2Version = "2.3.0"

libraryDependencies ++= Seq(
  "com.azure" % "azure-storage-blob"    % "12.6.0",
  "co.fs2"    %% "fs2-reactive-streams" % fs2Version
)
