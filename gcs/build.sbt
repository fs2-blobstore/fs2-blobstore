name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.72.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.137.0" % Test
)
