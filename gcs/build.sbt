name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.46.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.127.28" % Test
)
