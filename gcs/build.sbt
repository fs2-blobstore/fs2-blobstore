name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.35.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.127.11" % Test
)
