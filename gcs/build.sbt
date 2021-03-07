name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.113.12",
  "com.google.cloud" % "google-cloud-nio"     % "0.122.6" % Test
)
