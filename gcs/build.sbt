name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.3.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.20" % Test
)
