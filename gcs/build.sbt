name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.10.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.124.14" % Test
)
