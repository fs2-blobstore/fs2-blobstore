name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.115.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.1" % Test
)
