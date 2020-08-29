name := "azure"

val fs2Version = "2.4.4"

libraryDependencies ++= Seq(
  "com.azure" % "azure-storage-blob"   % "12.7.0",
  "co.fs2"   %% "fs2-reactive-streams" % fs2Version
)
