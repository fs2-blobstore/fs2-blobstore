name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.105.2",
  "com.google.cloud" % "google-cloud-nio"     % "0.120.0-alpha" % Test
)
