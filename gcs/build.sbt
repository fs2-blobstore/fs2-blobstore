name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.118.1",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.3" % Test
)
