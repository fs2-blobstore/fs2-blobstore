name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.6.1",
  "com.google.cloud" % "google-cloud-nio"     % "0.124.7" % Test
)
