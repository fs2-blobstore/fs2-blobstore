name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.30.1",
  "com.google.cloud" % "google-cloud-nio"     % "0.127.7" % Test
)
