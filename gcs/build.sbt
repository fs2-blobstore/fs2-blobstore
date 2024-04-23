name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.32.1",
  "com.google.cloud" % "google-cloud-nio"     % "0.127.16" % Test
)
