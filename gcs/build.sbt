name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.113.8",
  "com.google.cloud" % "google-cloud-nio"     % "0.122.3" % Test
)
