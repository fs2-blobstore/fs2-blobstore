name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.11.3",
  "com.google.cloud" % "google-cloud-nio"     % "0.124.8" % Test
)
