name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.113.16",
  "com.google.cloud" % "google-cloud-nio"     % "0.122.14" % Test
)
