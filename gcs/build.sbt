name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.6.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.25" % Test
)
