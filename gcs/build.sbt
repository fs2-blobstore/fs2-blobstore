name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.113.9",
  "com.google.cloud" % "google-cloud-nio"     % "0.122.5" % Test
)
