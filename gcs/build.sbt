name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.45.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.127.26" % Test
)
