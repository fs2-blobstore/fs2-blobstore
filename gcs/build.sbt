name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.26.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.127.0" % Test
)
