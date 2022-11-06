name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.13.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.124.19" % Test
)
