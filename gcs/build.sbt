name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.44.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.127.25" % Test
)
