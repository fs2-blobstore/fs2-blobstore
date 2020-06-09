name := "azure"

val fs2Version = "2.4.1"

libraryDependencies ++= Seq(
  "com.azure" % "azure-storage-blob"   % "12.6.1",
  "co.fs2"   %% "fs2-reactive-streams" % fs2Version
)
