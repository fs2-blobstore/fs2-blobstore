name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.58.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.128.5" % Test
)
