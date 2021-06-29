name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.117.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.2" % Test
)
