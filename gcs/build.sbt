name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.9.2",
  "com.google.cloud" % "google-cloud-nio"     % "0.124.7" % Test
)
