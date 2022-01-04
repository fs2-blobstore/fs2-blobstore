name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.2.2",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.17" % Test
)
